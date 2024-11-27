/***************************************************************
 *
 * Copyright (C) 2024, Pelican Project, Morgridge Institute for Research
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ***************************************************************/

#include "CurlUtil.hh"
#include "CurlWorker.hh"
#include "HTTPCommands.hh"
#include "logging.hh"

#include <XrdOuc/XrdOucCRC.hh>
#include <XrdSys/XrdSysPageSize.hh>

#include <sys/un.h>
#include <unistd.h>

#include <charconv>
#include <sstream>
#include <stdexcept>
#include <utility>

using namespace XrdHTTPServer;

thread_local std::vector<CURL *> HandlerQueue::m_handles;

HandlerQueue::HandlerQueue() {
	int filedes[2];
	auto result = pipe(filedes);
	if (result == -1) {
		throw std::runtime_error(strerror(errno));
	}
	m_read_fd = filedes[0];
	m_write_fd = filedes[1];
};

namespace {

// Simple debug function for getting information from libcurl; to enable, you
// need to recompile with GetHandle(true);
int dump_header(CURL *handle, curl_infotype type, char *data, size_t size,
				void *clientp) {
	(void)handle;
	(void)clientp;

	switch (type) {
	case CURLINFO_HEADER_OUT:
		printf("Header > %s\n", std::string(data, size).c_str());
		break;
	default:
		printf("Info: %s", std::string(data, size).c_str());
		break;
	}
	return 0;
}

} // namespace

CURL *GetHandle(bool verbose) {
	auto result = curl_easy_init();
	if (result == nullptr) {
		return result;
	}

	curl_easy_setopt(result, CURLOPT_USERAGENT, "xrootd-s3/devel");
	curl_easy_setopt(result, CURLOPT_DEBUGFUNCTION, dump_header);
	if (verbose)
		curl_easy_setopt(result, CURLOPT_VERBOSE, 1L);

	curl_easy_setopt(result, CURLOPT_BUFFERSIZE, 32 * 1024);

	return result;
}

CURL *HandlerQueue::GetHandle() {
	if (m_handles.size()) {
		auto result = m_handles.back();
		m_handles.pop_back();
		return result;
	}

	return ::GetHandle(false);
}

void HandlerQueue::RecycleHandle(CURL *curl) { m_handles.push_back(curl); }

void HandlerQueue::Produce(HTTPRequest *handler) {
	std::unique_lock<std::mutex> lk{m_mutex};
	m_cv.wait(lk, [&] { return m_ops.size() < m_max_pending_ops; });

	m_ops.push_back(handler);
	char ready[] = "1";
	while (true) {
		auto result = write(m_write_fd, ready, 1);
		if (result == -1) {
			if (errno == EINTR) {
				continue;
			}
			throw std::runtime_error(strerror(errno));
		}
		break;
	}

	lk.unlock();
	m_cv.notify_one();
}

HTTPRequest *HandlerQueue::Consume() {
	std::unique_lock<std::mutex> lk(m_mutex);
	m_cv.wait(lk, [&] { return m_ops.size() > 0; });

	auto result = std::move(m_ops.front());
	m_ops.pop_front();

	char ready[1];
	while (true) {
		auto result = read(m_read_fd, ready, 1);
		if (result == -1) {
			if (errno == EINTR) {
				continue;
			}
			throw std::runtime_error(strerror(errno));
		}
		break;
	}

	lk.unlock();
	m_cv.notify_one();

	return result;
}

HTTPRequest *HandlerQueue::TryConsume() {
	std::unique_lock<std::mutex> lk(m_mutex);
	if (m_ops.size() == 0) {
		return nullptr;
	}

	auto result = std::move(m_ops.front());
	m_ops.pop_front();

	char ready[1];
	while (true) {
		auto result = read(m_read_fd, ready, 1);
		if (result == -1) {
			if (errno == EINTR) {
				continue;
			}
			throw std::runtime_error(strerror(errno));
		}
		break;
	}

	lk.unlock();
	m_cv.notify_one();

	return result;
}

void CurlWorker::RunStatic(CurlWorker *myself) {
	try {
		myself->Run();
	} catch (...) {
		myself->m_logger.Log(LogMask::Debug, "CurlWorker::RunStatic",
							 "Curl worker got an exception");
	}
}

void CurlWorker::Run() {
	// Create a copy of the shared_ptr here.  Otherwise, when the main thread's
	// destructors run, there won't be any other live references to the
	// shared_ptr, triggering cleanup of the condition variable.  Because we
	// purposely don't shutdown the worker threads, those threads may be waiting
	// on the condition variable; destroying a condition variable while a thread
	// is waiting on it is undefined behavior.
	auto queue_ref = m_queue;
	auto &queue = *queue_ref.get();
	m_unpause_queue.reset(new HandlerQueue());
	m_logger.Log(LogMask::Debug, "Run", "Started a curl worker");

	CURLM *multi_handle = curl_multi_init();
	if (multi_handle == nullptr) {
		throw std::runtime_error("Failed to create curl multi-handle");
	}

	int running_handles = 0;
	time_t last_marker = time(NULL);
	CURLMcode mres = CURLM_OK;

	std::vector<struct curl_waitfd> waitfds;
	waitfds.resize(2);
	waitfds[0].fd = queue.PollFD();
	waitfds[0].events = CURL_WAIT_POLLIN;
	waitfds[0].revents = 0;
	waitfds[1].fd = m_unpause_queue->PollFD();
	waitfds[1].events = CURL_WAIT_POLLIN;
	waitfds[1].revents = 0;

	while (true) {
		while (running_handles < static_cast<int>(m_max_ops)) {
			auto op = m_unpause_queue->TryConsume();
			if (!op) {
				break;
			}
			op->ContinueHandle();
		}
		while (running_handles < static_cast<int>(m_max_ops)) {
			auto op =
				running_handles == 0 ? queue.Consume() : queue.TryConsume();
			if (!op) {
				break;
			}
			op->SetUnpauseQueue(m_unpause_queue);

			auto curl = queue.GetHandle();
			if (curl == nullptr) {
				m_logger.Log(LogMask::Debug, "Run",
							 "Unable to allocate a curl handle");
				op->Fail("E_NOMEM", "Unable to get allocate a curl handle");
				continue;
			}
			try {
				if (!op->SetupHandle(curl)) {
					op->Fail(op->getErrorCode(), op->getErrorMessage());
				}
			} catch (...) {
				m_logger.Log(LogMask::Debug, "Run",
							 "Unable to setup the curl handle");
				op->Fail("E_NOMEM",
						 "Failed to setup the curl handle for the operation");
				continue;
			}
			m_op_map[curl] = op;
			auto mres = curl_multi_add_handle(multi_handle, curl);
			if (mres != CURLM_OK) {
				if (m_logger.getMsgMask() & LogMask::Debug) {
					std::stringstream ss;
					ss << "Unable to add operation to the curl multi-handle: "
					   << curl_multi_strerror(mres);
					m_logger.Log(LogMask::Debug, "Run", ss.str().c_str());
				}
				op->Fail("E_CURL_LIB",
						 "Unable to add operation to the curl multi-handle");
				continue;
			}
			running_handles += 1;
		}

		// Maintain the periodic reporting of thread activity
		time_t now = time(NULL);
		time_t next_marker = last_marker + m_marker_period;
		if (now >= next_marker) {
			if (m_logger.getMsgMask() & LogMask::Debug) {
				std::stringstream ss;
				ss << "Curl worker thread " << getpid() << " is running "
				   << running_handles << " operations";
				m_logger.Log(LogMask::Debug, "CurlWorker", ss.str().c_str());
			}
			last_marker = now;
		}

		mres = curl_multi_wait(multi_handle, &waitfds[0], waitfds.size(), 50,
							   nullptr);
		if (mres != CURLM_OK) {
			if (m_logger.getMsgMask() & LogMask::Warning) {
				std::stringstream ss;
				ss << "Failed to wait on multi-handle: " << mres;
				m_logger.Log(LogMask::Warning, "CurlWorker", ss.str().c_str());
			}
		}

		// Do maintenance on the multi-handle
		int still_running;
		auto mres = curl_multi_perform(multi_handle, &still_running);
		if (mres == CURLM_CALL_MULTI_PERFORM) {
			continue;
		} else if (mres != CURLM_OK) {
			if (m_logger.getMsgMask() & LogMask::Warning) {
				std::stringstream ss;
				ss << "Failed to perform multi-handle operation: "
				   << curl_multi_strerror(mres);
				m_logger.Log(LogMask::Warning, "CurlWorker", ss.str().c_str());
			}
			break;
		}

		CURLMsg *msg;
		do {
			int msgq = 0;
			msg = curl_multi_info_read(multi_handle, &msgq);
			if (msg && (msg->msg == CURLMSG_DONE)) {
				auto iter = m_op_map.find(msg->easy_handle);
				if (iter == m_op_map.end()) {
					m_logger.Log(LogMask::Error, "CurlWorker",
								 "Logic error: got a callback for an entry "
								 "that doesn't exist");
					mres = CURLM_BAD_EASY_HANDLE;
					break;
				}
				auto &op = iter->second;
				auto res = msg->data.result;
				m_logger.Log(LogMask::Dump, "Run",
							 "Processing result from curl");
				op->ProcessCurlResult(iter->first, res);
				op->ReleaseHandle(iter->first);
				op->Notify();
				running_handles -= 1;
				curl_multi_remove_handle(multi_handle, iter->first);
				if (res == CURLE_OK) {
					// If the handle was successful, then we can recycle it.
					queue.RecycleHandle(iter->first);
				} else {
					curl_easy_cleanup(iter->first);
				}
				m_op_map.erase(iter);
			}
		} while (msg);
	}

	for (auto &map_entry : m_op_map) {
		map_entry.second->Fail("E_CURL_LIB", curl_multi_strerror(mres));
	}
	m_op_map.clear();
}
