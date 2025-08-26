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

#include "S3File.hh"
#include "CurlWorker.hh"
#include "S3Commands.hh"
#include "S3FileSystem.hh"
#include "logging.hh"
#include "stl_string_utils.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucStream.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSec/XrdSecEntityAttr.hh>
#include <XrdSfs/XrdSfsInterface.hh>
#include <XrdVersion.hh>
#include <XrdXrootd/XrdXrootdGStream.hh>

#include <curl/curl.h>

#include <algorithm>
#include <charconv>
#include <filesystem>
#include <inttypes.h>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdlib.h>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace XrdHTTPServer;

S3FileSystem *g_s3_oss = nullptr;

size_t S3File::m_cache_entry_size =
	(2 * 1024 * 1024); // Default size of the cache's buffer
XrdXrootdGStream *S3File::m_gstream = nullptr;

std::mutex S3File::m_shutdown_lock;
std::condition_variable S3File::m_shutdown_requested_cv;
bool S3File::m_shutdown_requested = false;
std::condition_variable S3File::m_shutdown_complete_cv;
bool S3File::m_shutdown_complete =
	true; // Starts in "true" state as the thread hasn't started

std::atomic<off_t> S3File::S3Cache::m_hit_bytes{0};
std::atomic<off_t> S3File::S3Cache::m_miss_bytes{0};
std::atomic<off_t> S3File::S3Cache::m_full_hit_count{0};
std::atomic<off_t> S3File::S3Cache::m_partial_hit_count{0};
std::atomic<off_t> S3File::S3Cache::m_miss_count{0};
std::atomic<off_t> S3File::S3Cache::m_bypass_bytes{0};
std::atomic<off_t> S3File::S3Cache::m_bypass_count{0};
std::atomic<off_t> S3File::S3Cache::m_fetch_bytes{0};
std::atomic<off_t> S3File::S3Cache::m_fetch_count{0};
std::atomic<off_t> S3File::S3Cache::m_unused_bytes{0};
std::atomic<off_t> S3File::S3Cache::m_prefetch_bytes{0};
std::atomic<off_t> S3File::S3Cache::m_prefetch_count{0};
std::atomic<off_t> S3File::S3Cache::m_errors{0};
std::atomic<std::chrono::steady_clock::duration::rep>
	S3File::S3Cache::m_bypass_duration{0};
std::atomic<std::chrono::steady_clock::duration::rep>
	S3File::S3Cache::m_fetch_duration{0};

XrdVERSIONINFO(XrdOssGetFileSystem, S3);

std::vector<std::pair<std::weak_ptr<std::mutex>,
					  std::weak_ptr<AmazonS3SendMultipartPart>>>
	S3File::m_pending_ops;
std::mutex S3File::m_pending_lk;
std::once_flag S3File::m_monitor_launch;

S3File::S3File(XrdSysError &log, S3FileSystem *oss) : m_log(log), m_oss(oss) {}

int S3File::Open(const char *path, int Oflag, mode_t Mode, XrdOucEnv &env) {
	if (m_is_open) {
		m_log.Log(LogMask::Warning, "Open", "File already open:", path);
		return -EBADF;
	}
	if (Oflag & O_CREAT) {
		m_log.Log(LogMask::Info, "Open", "File opened for creation:", path);
		m_create = true;
	}
	if (Oflag & O_APPEND) {
		m_log.Log(LogMask::Info, "Open", "File opened for append:", path);
	}
	if ((Oflag & O_ACCMODE) != O_RDONLY) {
		m_write_lk.reset(new std::mutex);
	}

	char *asize_char;
	if ((asize_char = env.Get("oss.asize"))) {
		off_t result{0};
		auto [ptr, ec] = std::from_chars(
			asize_char, asize_char + strlen(asize_char), result);
		if (ec == std::errc()) {
			m_object_size = result;
		} else {
			m_log.Log(LogMask::Warning,
					  "Opened file has oss.asize set to an unparseable value: ",
					  asize_char);
		}
	}

	if (m_log.getMsgMask() & XrdHTTPServer::Debug) {
		m_log.Log(LogMask::Warning, "S3File::Open", "Opening file", path);
	}

	std::string exposedPath, object;
	auto rv = m_oss->parsePath(path, exposedPath, object);
	if (rv != 0) {
		return rv;
	}
	auto ai = m_oss->getS3AccessInfo(exposedPath, object);
	if (!ai) {
		return -ENOENT;
	}
	if (ai->getS3BucketName().empty()) {
		return -EINVAL;
	}
	if (object == "") {
		return -ENOENT;
	}

	m_ai = *ai;
	m_object = object;

	// This flag is not set when it's going to be a read operation
	// so we check if the file exists in order to be able to return a 404
	if (((Oflag & O_ACCMODE) == O_RDONLY) || (Oflag & O_APPEND)) {
		auto res = Fstat(nullptr);
		if (res < 0) {
			return res;
		}
	}

	m_is_open = true;
	return 0;
}

ssize_t S3File::ReadV(XrdOucIOVec *readV, int rdvcnt) {
	if (!m_is_open) {
		m_log.Log(LogMask::Warning, "Write", "File not open");
		return -EBADF;
	}

	if (rdvcnt <= 0 || !readV) {
		return -EINVAL;
	}

	size_t totalRead = 0;
	for (int i = 0; i < rdvcnt; ++i) {
		auto &iov = readV[i];
		if (iov.size == 0) {
			continue;
		}
		auto bytesRead =
			Read(static_cast<void *>(iov.data), iov.offset, iov.size);
		if (bytesRead < 0) {
			return bytesRead;
		} else if (bytesRead != iov.size) {
			// Error number copied from implementation in XrdOss/XrdOssApi.cc
			return -ESPIPE;
		}
		totalRead += bytesRead;
	}
	return totalRead;
}

ssize_t S3File::Read(void *buffer, off_t offset, size_t size) {
	if (!m_is_open) {
		m_log.Log(LogMask::Warning, "Write", "File not open");
		return -EBADF;
	}

	return m_cache.Read(static_cast<char *>(buffer), offset, size);
}

int S3File::Fstat(struct stat *buff) {

	if (content_length == -1) {
		AmazonS3Head head(m_ai, m_object, m_log);
		if (!head.SendRequest()) {
			auto httpCode = head.getResponseCode();
			if (httpCode) {
				std::stringstream ss;
				ss << "HEAD command failed: " << head.getResponseCode() << ": "
				   << head.getResultString();
				m_log.Log(LogMask::Warning, "S3ile::Fstat", ss.str().c_str());
				switch (httpCode) {
				case 404:
					return -ENOENT;
				case 500:
					return -EIO;
				case 403:
					return -EPERM;
				default:
					return -EIO;
				}
			} else {
				std::stringstream ss;
				ss << "Failed to send HEAD command: " << head.getErrorCode()
				   << ": " << head.getErrorMessage();
				m_log.Log(LogMask::Warning, "S3File::Fstat", ss.str().c_str());
				return -EIO;
			}
		}

		content_length = head.getSize();
		last_modified = head.getLastModified();
		if (content_length < 0) {
			m_log.Log(LogMask::Warning, "S3File::Fstat",
					  "Returned content length is negative");
			return -EINVAL;
		}
	}

	if (buff) {
		memset(buff, '\0', sizeof(struct stat));
		buff->st_mode = 0600 | S_IFREG;
		buff->st_nlink = 1;
		buff->st_uid = 1;
		buff->st_gid = 1;
		buff->st_size = content_length;
		buff->st_mtime = last_modified;
		buff->st_atime = 0;
		buff->st_ctime = 0;
		buff->st_dev = 0;
		buff->st_ino = 0;
	}

	return 0;
}

ssize_t S3File::Write(const void *buffer, off_t offset, size_t size) {
	if (!m_is_open) {
		m_log.Log(LogMask::Warning, "Write", "File not open");
		return -EBADF;
	}

	auto write_mutex = m_write_lk;
	if (!write_mutex) {
		return -EBADF;
	}
	std::lock_guard lk(*write_mutex);

	// Small object optimization -- if this is the full object, upload
	// it immediately.
	if (!m_write_offset && m_object_size == static_cast<off_t>(size)) {
		AmazonS3Upload upload(m_ai, m_object, m_log);
		m_write_lk.reset();
		if (!upload.SendRequest(
				std::string_view(static_cast<const char *>(buffer), size))) {
			m_log.Log(LogMask::Warning, "Write",
					  "Failed to create small object");
			return -EIO;
		} else {
			m_write_offset += size;
			m_log.Log(LogMask::Debug, "Write",
					  "Creation of small object succeeded",
					  std::to_string(size).c_str());
			return size;
		}
	}

	if (offset != m_write_offset) {
		m_log.Emsg(
			"Write",
			"Out-of-order write detected; S3 requires writes to be in order");
		m_write_offset = -1;
		return -EIO;
	}
	if (m_write_offset == -1) {
		// Previous I/O error has occurred.  File is in bad state, immediately
		// fail.
		return -EIO;
	}
	if (uploadId == "") {
		AmazonS3CreateMultipartUpload startUpload(m_ai, m_object, m_log);
		if (!startUpload.SendRequest()) {
			m_log.Emsg("Write", "S3 multipart request failed");
			m_write_offset = -1;
			return -ENOENT;
		}
		std::string errMsg;
		startUpload.Results(uploadId, errMsg);
	}

	// If we don't know the final object size, we must use the streaming
	// variant.
	if (m_object_size == -1) {
		return WriteStreaming(buffer, offset, size);
	}

	size_t written = 0;
	while (written != size) {
		if (m_write_op) {
			auto write_size = ContinueSendPart(buffer, size);
			if (write_size < 0) {
				return write_size;
			}
			offset += write_size;
			m_write_offset += write_size;
			buffer = static_cast<const char *>(buffer) + write_size;
			size -= write_size;
			written += write_size;
			if (!size) {
				return written;
			}
		}

		m_write_op.reset(new AmazonS3SendMultipartPart(m_ai, m_object, m_log));
		{
			std::lock_guard lk(m_pending_lk);
			m_pending_ops.emplace_back(m_write_lk, m_write_op);
		}

		// Calculate the size of the current chunk, if it's known.
		m_part_size = m_s3_part_size;
		if (!m_object_size) {
			m_part_size = 0;
		} else if (m_write_offset + static_cast<off_t>(m_part_size) >
				   m_object_size) {
			m_part_size = m_object_size - m_write_offset;
		}
	}
	return written;
}

ssize_t S3File::WriteStreaming(const void *buffer, off_t offset, size_t size) {
	m_streaming_buffer.append(
		std::string_view(static_cast<const char *>(buffer), size));
	m_write_offset += size;

	ssize_t rv = size;
	if (m_streaming_buffer.size() > 100'000'000) {
		rv = SendPartStreaming();
	}
	return rv;
}

ssize_t S3File::SendPartStreaming() {
	int length = m_streaming_buffer.length();
	AmazonS3SendMultipartPart upload_part_request =
		AmazonS3SendMultipartPart(m_ai, m_object, m_log);
	if (!upload_part_request.SendRequest(m_streaming_buffer,
										 std::to_string(partNumber), uploadId,
										 m_streaming_buffer.size(), true)) {
		m_log.Log(LogMask::Debug, "SendPart", "upload.SendRequest() failed");
		return -EIO;
	} else {
		m_log.Log(LogMask::Debug, "SendPart", "upload.SendRequest() succeeded");
		std::string etag;
		if (!upload_part_request.GetEtag(etag)) {
			m_log.Log(
				LogMask::Debug, "SendPart",
				"upload.SendRequest() response missing an eTag in response");
			return -EIO;
		}
		eTags.push_back(etag);
		partNumber++;
		m_streaming_buffer.clear();
	}

	return length;
}

ssize_t S3File::ContinueSendPart(const void *buffer, size_t size) {
	m_part_written += size;
	auto write_size = size;
	if (m_part_written > m_s3_part_size) {
		write_size = size - (m_part_written - m_s3_part_size);
		m_part_written = m_s3_part_size;
	}
	auto is_final = (m_part_size > 0 && m_part_written == m_part_size) ||
					m_part_written == m_s3_part_size;
	if (m_log.getMsgMask() & LogMask::Debug) {
		std::stringstream ss;
		ss << "Sending request with buffer of size=" << write_size
		   << ", offset=" << m_write_offset << " and is_final=" << is_final;
		m_log.Log(LogMask::Debug, "ContinueSendPart", ss.str().c_str());
	}
	if (!m_write_op->SendRequest(
			std::string_view(static_cast<const char *>(buffer), write_size),
			std::to_string(partNumber), uploadId, m_object_size, is_final)) {
		m_write_offset = -1;
		if (m_write_op->getErrorCode() == "E_TIMEOUT") {
			m_log.Emsg("Write", "Timeout when uploading to S3");
			m_write_op.reset();
			return -ETIMEDOUT;
		}
		m_log.Emsg("Write", "Upload to S3 failed: ",
				   m_write_op->getErrorMessage().c_str());
		m_write_op.reset();
		return -EIO;
	}
	if (is_final) {
		m_part_written = 0;
		m_part_size = 0;
		std::string etag;
		if (!m_write_op->GetEtag(etag)) {
			m_log.Emsg("Write", "Result from S3 does not include ETag");
			m_write_op.reset();
			m_write_offset = -1;
			return -EIO;
		}
		eTags.push_back(etag);
		m_write_op.reset();
		partNumber++;
	}

	return write_size;
}

void S3File::LaunchMonitorThread(XrdSysError &log, XrdOucEnv *envP) {
	std::call_once(m_monitor_launch, [&] {
		if (envP) {
			m_gstream = reinterpret_cast<XrdXrootdGStream *>(
				envP->GetPtr("oss.gStream*"));
			if (m_gstream) {
				log.Say("Config", "S3 OSS monitoring has been configured via "
								  "xrootd.mongstream directive");
			} else {
				log.Say(
					"Config",
					"S3 OSS plugin is not configured to send statistics; "
					"use `xrootd.mongstream oss ...` directive to enable it");
			}
		} else {
			log.Say("Config", "XrdOssStats plugin invoked without a configured "
							  "environment; likely an internal error");
		}
		m_shutdown_complete = false;
		std::thread t(S3File::Maintenance, std::ref(log));
		t.detach();
	});
}

void S3File::Maintenance(XrdSysError &log) {
	auto sleep_duration = HTTPRequest::GetStallTimeout() / 3;
	if (sleep_duration > std::chrono::seconds(1)) {
		sleep_duration = std::chrono::seconds(1);
	}
	while (true) {

		{
			std::unique_lock lock(m_shutdown_lock);
			m_shutdown_requested_cv.wait_for(
				lock, sleep_duration, [] { return m_shutdown_requested; });
			if (m_shutdown_requested) {
				break;
			}
		}

		try {
			CleanupTransfersOnce();
		} catch (std::exception &exc) {
			std::cerr << "Warning: caught unexpected exception when trying to "
						 "clean transfers: "
					  << exc.what() << std::endl;
		}
		try {
			SendStatistics(log);
		} catch (std::exception &exc) {
			std::cerr << "Warning: caught unexpected exception when trying to "
						 "send statistics: "
					  << exc.what() << std::endl;
		}
	}
	std::unique_lock lock(m_shutdown_lock);
	m_shutdown_complete = true;
	m_shutdown_complete_cv.notify_one();
}

void S3File::SendStatistics(XrdSysError &log) {
	char buf[1500];
	auto bypass_duration_count =
		S3Cache::m_bypass_duration.load(std::memory_order_relaxed);
	auto fetch_duration_count =
		S3Cache::m_fetch_duration.load(std::memory_order_relaxed);
	std::chrono::steady_clock::duration bypass_duration{
		std::chrono::steady_clock::duration::rep(bypass_duration_count)};
	std::chrono::steady_clock::duration fetch_duration{
		std::chrono::steady_clock::duration::rep(fetch_duration_count)};
	auto bypass_s = std::chrono::duration_cast<std::chrono::duration<float>>(
						bypass_duration)
						.count();
	auto fetch_s =
		std::chrono::duration_cast<std::chrono::duration<float>>(fetch_duration)
			.count();
	auto len = snprintf(
		buf, 500,
		"{"
		"\"event\":\"s3file_stats\","
		"\"hit_b\":%" PRIu64 ",\"miss_b\":%" PRIu64 ",\"full_hit\":%" PRIu64 ","
		"\"part_hit\":%" PRIu64 ",\"miss\":%" PRIu64 ",\"bypass_b\":%" PRIu64
		","
		"\"bypass\":%" PRIu64 ",\"fetch_b\":%" PRIu64 ",\"fetch\":%" PRIu64 ","
		"\"unused_b\":%" PRIu64 ",\"prefetch_b\":%" PRIu64
		",\"prefetch\":%" PRIu64 ","
		"\"errors\":%" PRIu64 ",\"bypass_s\":%.3f,\"fetch_s\":%.3f"
		"}",
		static_cast<uint64_t>(
			S3Cache::m_hit_bytes.load(std::memory_order_relaxed)),
		static_cast<uint64_t>(
			S3Cache::m_miss_bytes.load(std::memory_order_relaxed)),
		static_cast<uint64_t>(
			S3Cache::m_full_hit_count.load(std::memory_order_relaxed)),
		static_cast<uint64_t>(
			S3Cache::m_partial_hit_count.load(std::memory_order_relaxed)),
		static_cast<uint64_t>(
			S3Cache::m_miss_count.load(std::memory_order_relaxed)),
		static_cast<uint64_t>(
			S3Cache::m_bypass_bytes.load(std::memory_order_relaxed)),
		static_cast<uint64_t>(
			S3Cache::m_bypass_count.load(std::memory_order_relaxed)),
		static_cast<uint64_t>(
			S3Cache::m_fetch_bytes.load(std::memory_order_relaxed)),
		static_cast<uint64_t>(
			S3Cache::m_fetch_count.load(std::memory_order_relaxed)),
		static_cast<uint64_t>(
			S3Cache::m_unused_bytes.load(std::memory_order_relaxed)),
		static_cast<uint64_t>(
			S3Cache::m_prefetch_bytes.load(std::memory_order_relaxed)),
		static_cast<uint64_t>(
			S3Cache::m_prefetch_count.load(std::memory_order_relaxed)),
		static_cast<uint64_t>(
			S3Cache::m_errors.load(std::memory_order_relaxed)),
		bypass_s, fetch_s);
	if (len >= 500) {
		log.Log(LogMask::Error, "Statistics",
				"Failed to generate g-stream statistics packet");
		return;
	}
	log.Log(LogMask::Debug, "Statistics", buf);
	if (m_gstream && !m_gstream->Insert(buf, len + 1)) {
		log.Log(LogMask::Error, "Statistics",
				"Failed to send g-stream statistics packet");
		return;
	}
}

void S3File::CleanupTransfersOnce() {
	// Make a list of live transfers; erase any dead ones still on the list.
	std::vector<std::pair<std::shared_ptr<std::mutex>,
						  std::shared_ptr<AmazonS3SendMultipartPart>>>
		existing_ops;
	{
		std::lock_guard lk(m_pending_lk);
		existing_ops.reserve(m_pending_ops.size());
		m_pending_ops.erase(
			std::remove_if(m_pending_ops.begin(), m_pending_ops.end(),
						   [&](const auto &op) -> bool {
							   auto op_lk = op.first.lock();
							   if (!op_lk) {
								   // In this case, the S3File is no longer open
								   // for write.  No need to potentially clean
								   // up the transfer.
								   return true;
							   }
							   auto op_part = op.second.lock();
							   if (!op_part) {
								   // In this case, the S3File object is still
								   // open for writes but the upload has
								   // completed. Remove from the list.
								   return true;
							   }
							   // The S3File is open and upload is in-progress;
							   // we'll tick the transfer.
							   existing_ops.emplace_back(op_lk, op_part);
							   return false;
						   }),
			m_pending_ops.end());
	}
	// For each live transfer, call `Tick` to advance the clock and possibly
	// time things out.
	auto now = std::chrono::steady_clock::now();
	for (auto &info : existing_ops) {
		std::lock_guard lk(*info.first);
		info.second->Tick(now);
	}
}

int S3File::Close(long long *retsz) {
	if (!m_is_open) {
		m_log.Log(LogMask::Warning, "Close", "File not open");
		return -EBADF;
	}
	m_is_open = false;

	// If we opened the object in create mode but did not actually write
	// anything, make a quick zero-length file.
	if (m_create && !m_write_offset) {
		AmazonS3Upload upload(m_ai, m_object, m_log);
		if (!upload.SendRequest("")) {
			m_log.Log(LogMask::Warning, "Close",
					  "Failed to create zero-length object");
			return -ENOENT;
		} else {
			m_log.Log(LogMask::Debug, "Close",
					  "Creation of zero-length object succeeded");
			return 0;
		}
	}
	if (m_write_lk) {
		std::lock_guard lk(*m_write_lk);
		if (m_object_size == -1 && !m_streaming_buffer.empty()) {
			m_log.Emsg("Close", "Sending final part of length",
					   std::to_string(m_streaming_buffer.size()).c_str());
			auto rv = SendPartStreaming();
			if (rv < 0) {
				return rv;
			}
		} else if (m_write_op) {
			m_part_size = m_part_written;
			auto written = ContinueSendPart(nullptr, 0);
			if (written < 0) {
				m_log.Log(LogMask::Warning, "Close",
						  "Failed to complete the last S3 upload");
				return -EIO;
			}
		}
	}

	// this is only true if some parts have been written and need to be
	// finalized
	if (partNumber > 1) {
		AmazonS3CompleteMultipartUpload complete_upload_request =
			AmazonS3CompleteMultipartUpload(m_ai, m_object, m_log);
		if (!complete_upload_request.SendRequest(eTags, partNumber, uploadId)) {
			m_log.Emsg("SendPart", "close.SendRequest() failed");
			return -ENOENT;
		} else {
			m_log.Emsg("SendPart", "close.SendRequest() succeeded");
		}
	}

	return 0;
}

// Copy any overlapping data from the cache buffer into the request buffer,
// returning the remaining data necessary to fill the request.
//
// - `req_off`: File offset of the beginning of the request buffer.
// - `req_size`: Size of the request buffer
// - `req_buf`: Request buffer to copy data into
// - `cache_off`: File offset of the beginning of the cache buffer.
// - `cache_size`: Size of the cache buffer
// - `cache_buf`: Cache buffer to copy data from.
// - `used` (output): Incremented by the number of bytes copied from the cache
// buffer
// - Returns the (offset, size) of the remaining reads needed to satisfy the
// request. If there is only one (or no!) remaining reads, then the
// corresponding tuple returned is (-1, 0).
std::tuple<off_t, size_t, off_t, size_t>
OverlapCopy(off_t req_off, size_t req_size, char *req_buf, off_t cache_off,
			size_t cache_size, char *cache_buf, size_t &used) {
	if (req_off < 0) {
		return std::make_tuple(req_off, req_size, -1, 0);
	}
	if (cache_off < 0) {
		return std::make_tuple(req_off, req_size, -1, 0);
	}

	if (cache_off <= req_off) {
		auto cache_end = cache_off + static_cast<off_t>(cache_size);
		if (cache_end > req_off) {
			auto cache_buf_off = static_cast<size_t>(req_off - cache_off);
			auto cache_copy_bytes =
				std::min(static_cast<size_t>(cache_end - req_off), req_size);
			memcpy(req_buf, cache_buf + cache_buf_off, cache_copy_bytes);
			used += cache_copy_bytes;
			return std::make_tuple(req_off + cache_copy_bytes,
								   req_size - cache_copy_bytes, -1, 0);
		}
	}
	if (req_off < cache_off) {
		auto req_end = static_cast<off_t>(req_off + req_size);
		if (req_end > cache_off) {
			auto req_buf_off = static_cast<size_t>(cache_off - req_off);
			auto cache_end = static_cast<off_t>(cache_off + cache_size);
			auto trailing_bytes = static_cast<off_t>(req_end - cache_end);
			if (trailing_bytes > 0) {
				memcpy(req_buf + req_buf_off, cache_buf, cache_size);
				used += cache_size;
				return std::make_tuple(req_off, req_buf_off, cache_end,
									   trailing_bytes);
			}
			memcpy(req_buf + req_buf_off, cache_buf, req_end - cache_off);
			used += req_end - cache_off;
			return std::make_tuple(req_off, req_buf_off, -1, 0);
		}
	}
	return std::make_tuple(req_off, req_size, -1, 0);
}

std::tuple<off_t, size_t, off_t, size_t>
S3File::S3Cache::Entry::OverlapCopy(off_t req_off, size_t req_size,
									char *req_buf, bool is_hit) {
	size_t bytes_copied = 0;
	auto results =
		::OverlapCopy(req_off, req_size, req_buf, m_off, m_cache_entry_size,
					  m_data.data(), bytes_copied);
	if (is_hit) {
		m_parent.m_hit_bytes.fetch_add(bytes_copied, std::memory_order_relaxed);
	}
	m_used += bytes_copied;
	return results;
}

std::tuple<off_t, size_t, bool>
S3File::DownloadBypass(off_t offset, size_t size, char *buffer) {
	if (m_cache_entry_size && (size <= m_cache_entry_size)) {
		return std::make_tuple(offset, size, false);
	}
	AmazonS3Download download(m_ai, m_object, m_log, buffer);
	auto start = std::chrono::steady_clock::now();
	auto result = download.SendRequest(offset, size);
	auto duration = std::chrono::steady_clock::now() - start;
	m_cache.m_bypass_duration.fetch_add(duration.count(),
										std::memory_order_relaxed);
	if (!result) {
		std::stringstream ss;
		ss << "Failed to send GetObject command: " << download.getResponseCode()
		   << "'" << download.getResultString() << "'";
		m_log.Log(LogMask::Warning, "S3File::Read", ss.str().c_str());
		return std::make_tuple(0, -1, false);
	}
	return std::make_tuple(-1, 0, true);
}

S3File::S3Cache::~S3Cache() {
	std::unique_lock lk(m_mutex);
	m_cv.wait(lk, [&] { return !m_a.m_inprogress && !m_b.m_inprogress; });
}

bool S3File::S3Cache::CouldUseAligned(off_t req, off_t cache) {
	if (req < 0 || cache < 0) {
		return false;
	}
	return (req >= cache) &&
		   (req < cache + static_cast<off_t>(S3File::m_cache_entry_size));
}

bool S3File::S3Cache::CouldUse(off_t req_off, size_t req_size,
							   off_t cache_off) {
	if (req_off < 0 || cache_off < 0) {
		return false;
	}
	auto cache_end = cache_off + static_cast<off_t>(m_cache_entry_size);
	if (req_off >= cache_off) {
		return req_off < cache_end;
	} else {
		return req_off + static_cast<off_t>(req_size) > cache_off;
	}
}

void S3File::S3Cache::DownloadCaches(bool download_a, bool download_b,
									 bool locked) {
	if (!download_a && !download_b) {
		return;
	}

	std::unique_lock lk(m_mutex, std::defer_lock);
	if (!locked) {
		lk.lock();
	}
	if (download_a) {
		m_a.Download(m_parent);
	}
	if (download_b) {
		m_b.Download(m_parent);
	}
}

ssize_t S3File::S3Cache::Read(char *buffer, off_t offset, size_t size) {
	if (offset >= m_parent.content_length) {
		return 0;
	}
	if (offset + static_cast<off_t>(size) > m_parent.content_length) {
		size = m_parent.content_length - offset;
	}
	if (m_parent.m_log.getMsgMask() & LogMask::Debug) {
		std::stringstream ss;
		ss << "Read request for object=" << m_parent.m_object
		   << ", offset=" << offset << ", size=" << size;
		m_parent.m_log.Log(LogMask::Debug, "cache", ss.str().c_str());
	}

	off_t req3_off, req4_off, req5_off, req6_off;
	size_t req3_size, req4_size, req5_size, req6_size;
	// Copy as much data out of the cache as possible; wait for the caches to
	// finish their downloads if a cache fill is in progress and we could
	// utilize the cache fill.
	if (m_cache_entry_size) {
		std::unique_lock lk{m_mutex};
		if (m_a.m_inprogress) {
			m_cv.wait(lk, [&] {
				return !m_a.m_inprogress || !CouldUse(offset, size, m_a.m_off);
			});
		}
		off_t req1_off, req2_off;
		size_t req1_size, req2_size;
		std::tie(req1_off, req1_size, req2_off, req2_size) =
			m_a.OverlapCopy(offset, size, buffer, true);
		if (m_b.m_inprogress) {
			m_cv.wait(lk, [&] {
				return !m_b.m_inprogress ||
					   !(CouldUse(req1_off, req1_size, m_b.m_off) ||
						 CouldUse(req2_off, req2_size, m_b.m_off));
			});
		}
		std::tie(req3_off, req3_size, req4_off, req4_size) = m_b.OverlapCopy(
			req1_off, req1_size, buffer + req1_off - offset, true);
		std::tie(req5_off, req5_size, req6_off, req6_size) = m_b.OverlapCopy(
			req2_off, req2_size, buffer + req2_off - offset, true);
	} else {
		auto [off_next, size_next, downloaded] =
			m_parent.DownloadBypass(offset, size, buffer);
		if (!downloaded) {
			m_parent.m_log.Log(LogMask::Warning, "S3File::Read",
							   "Failed to download data bypassing the cache");
			m_errors.fetch_add(1, std::memory_order_relaxed);
			return -1;
		} else {
			m_bypass_bytes.fetch_add(size, std::memory_order_relaxed);
			m_bypass_count.fetch_add(1, std::memory_order_relaxed);
			return size;
		}
	}
	// If any of the remaining missing bytes are bigger than a single chunk,
	// download those bypassing the cache.
	bool downloaded;
	size_t bypass_size = req3_size;
	std::tie(req3_off, req3_size, downloaded) = m_parent.DownloadBypass(
		req3_off, req3_size, buffer + req3_off - offset);
	if (req3_size < 0) {
		m_errors.fetch_add(1, std::memory_order_relaxed);
		return -1;
	}
	if (downloaded) {
		m_bypass_bytes.fetch_add(bypass_size, std::memory_order_relaxed);
		m_bypass_count.fetch_add(1, std::memory_order_relaxed);
	}
	bypass_size = req4_size;
	std::tie(req4_off, req4_size, downloaded) = m_parent.DownloadBypass(
		req4_off, req4_size, buffer + req4_off - offset);
	if (req4_size < 0) {
		m_errors.fetch_add(1, std::memory_order_relaxed);
		return -1;
	}
	if (downloaded) {
		m_bypass_bytes.fetch_add(bypass_size, std::memory_order_relaxed);
		m_bypass_count.fetch_add(1, std::memory_order_relaxed);
	}
	bypass_size = req5_size;
	std::tie(req5_off, req5_size, downloaded) = m_parent.DownloadBypass(
		req5_off, req5_size, buffer + req5_off - offset);
	if (req5_size < 0) {
		m_errors.fetch_add(1, std::memory_order_relaxed);
		return -1;
	}
	if (downloaded) {
		m_bypass_bytes.fetch_add(bypass_size, std::memory_order_relaxed);
		m_bypass_count.fetch_add(1, std::memory_order_relaxed);
	}
	bypass_size = req6_size;
	std::tie(req6_off, req6_size, downloaded) = m_parent.DownloadBypass(
		req6_off, req6_size, buffer + req6_off - offset);
	if (req6_size < 0) {
		m_errors.fetch_add(1, std::memory_order_relaxed);
		return -1;
	}
	if (downloaded) {
		m_bypass_bytes.fetch_add(bypass_size, std::memory_order_relaxed);
		m_bypass_count.fetch_add(1, std::memory_order_relaxed);
	}
	if (req3_size == 0 && req4_size == 0 && req5_size == 0 && req6_size == 0) {
		m_full_hit_count.fetch_add(1, std::memory_order_relaxed);
		// We've used more bytes in the cache, potentially all of the bytes.
		// In that case, we could drop one of the cache entries and prefetch
		// more of the object.
		bool download_a = false, download_b = false;
		{
			std::unique_lock lk{m_mutex};
			auto next_offset = std::max(m_a.m_off, m_b.m_off) +
							   static_cast<off_t>(m_cache_entry_size);
			if (next_offset < m_parent.content_length) {
				if (!m_a.m_inprogress && m_a.m_used >= m_cache_entry_size) {
					m_a.m_inprogress = true;
					m_a.m_off = next_offset;
					download_a = true;
					next_offset += m_cache_entry_size;
				}
				if (!m_b.m_inprogress && m_b.m_used >= m_cache_entry_size) {
					m_b.m_inprogress = true;
					m_b.m_off = next_offset;
					download_b = true;
				}
			}
		}
		if (download_a) {
			size_t request_size = m_cache_entry_size;
			if (m_a.m_off + static_cast<off_t>(request_size) >
				m_parent.content_length) {
				request_size = m_parent.content_length - m_a.m_off;
			}
			m_prefetch_count.fetch_add(1, std::memory_order_relaxed);
			m_prefetch_bytes.fetch_add(request_size, std::memory_order_relaxed);
		}
		if (download_b) {
			size_t request_size = m_cache_entry_size;
			if (m_b.m_off + static_cast<off_t>(request_size) >
				m_parent.content_length) {
				request_size = m_parent.content_length - m_b.m_off;
			}
			m_prefetch_count.fetch_add(1, std::memory_order_relaxed);
			m_prefetch_bytes.fetch_add(request_size, std::memory_order_relaxed);
		}
		DownloadCaches(download_a, download_b, false);
		return size;
	}
	// At this point, the only remaining data requests must be less than the
	// size of the cache chunk, implying it's a partial request at the beginning
	// or end of the range -- hence only two can exist.
	off_t req1_off = -1, req2_off = -1;
	off_t *req_off = &req1_off;
	size_t req1_size = 0, req2_size = 0;
	size_t *req_size = &req1_size;
	if (req3_off != -1) {
		*req_off = req3_off;
		*req_size = req3_size;
		req_off = &req2_off;
		req_size = &req2_size;
	}
	if (req4_off != -1) {
		*req_off = req4_off;
		*req_size = req4_size;
		req_off = &req2_off;
		req_size = &req2_size;
	}
	if (req5_off != -1) {
		*req_off = req5_off;
		*req_size = req5_size;
		req_off = &req2_off;
		req_size = &req2_size;
	}
	if (req6_off != -1) {
		*req_off = req6_off;
		*req_size = req6_size;
	}
	if (req1_off != -1 && req2_off == -1) {
		auto chunk_off = static_cast<off_t>(req1_off / m_cache_entry_size *
												m_cache_entry_size +
											m_cache_entry_size);
		auto req_end = static_cast<off_t>(req1_off + req1_size);

		if (req_end > chunk_off) {
			req2_off = chunk_off;
			req2_size = req_end - chunk_off;
			req1_size = chunk_off - req1_off;
		}
	}
	size_t miss_bytes = req1_size + req2_size;
	if (miss_bytes == size) {
		m_miss_count.fetch_add(1, std::memory_order_relaxed);
	} else {
		m_partial_hit_count.fetch_add(1, std::memory_order_relaxed);
	}
	m_miss_bytes.fetch_add(miss_bytes, std::memory_order_relaxed);
	while (req1_off != -1) {
		std::unique_lock lk(m_mutex);
		m_cv.wait(lk, [&] {
			bool req1waitOnA =
				m_a.m_inprogress && CouldUseAligned(req1_off, m_a.m_off);
			bool req2waitOnA =
				m_a.m_inprogress && CouldUseAligned(req2_off, m_a.m_off);
			bool req1waitOnB =
				m_b.m_inprogress && CouldUseAligned(req1_off, m_b.m_off);
			bool req2waitOnB =
				m_b.m_inprogress && CouldUseAligned(req2_off, m_b.m_off);
			// If there's an idle cache entry, use it -- unless the other cache
			// entry is working on this request.
			if (!m_a.m_inprogress && !req1waitOnB && !req2waitOnB) {
				return true;
			}
			if (!m_b.m_inprogress && !req1waitOnA && !req2waitOnA) {
				return true;
			}
			// If an idle cache entry can immediately satisfy the request, we
			// use it.
			if (!m_a.m_inprogress && (CouldUseAligned(req1_off, m_a.m_off) ||
									  CouldUseAligned(req2_off, m_a.m_off))) {
				return true;
			}
			if (!m_b.m_inprogress && (CouldUseAligned(req1_off, m_b.m_off) ||
									  CouldUseAligned(req2_off, m_b.m_off))) {
				return true;
			}
			// If either request is in progress, we continue to wait.
			if (req1waitOnA || req1waitOnB || req2waitOnA || req2waitOnB) {
				return false;
			}
			// If either cache is idle, we will use it.
			return !m_a.m_inprogress || !m_b.m_inprogress;
		});
		// std::cout << "A entry in progress: " << m_a.m_inprogress
		//		  << ", with offset " << m_a.m_off << "\n";
		// std::cout << "B entry in progress: " << m_b.m_inprogress
		//		  << ", with offset " << m_b.m_off << "\n";
		// Test to see if any of the buffers could immediately fulfill the
		// requests.
		auto consumed_req = false;
		if (!m_a.m_inprogress) {
			if (CouldUseAligned(req2_off, m_a.m_off)) {
				if (m_a.m_failed) {
					m_a.m_failed = false;
					m_a.m_off = -1;
					m_errors.fetch_add(1, std::memory_order_relaxed);
					return -1;
				}
				m_a.OverlapCopy(req2_off, req2_size, buffer + req2_off - offset,
								false);
				req2_off = -1;
				req2_size = 0;
				consumed_req = true;
			}
			if (CouldUseAligned(req1_off, m_a.m_off)) {
				if (m_a.m_failed) {
					m_a.m_failed = false;
					m_a.m_off = -1;
					m_errors.fetch_add(1, std::memory_order_relaxed);
					return -1;
				}
				m_a.OverlapCopy(req1_off, req1_size, buffer + req1_off - offset,
								false);
				req1_off = req2_off;
				req1_size = req2_size;
				req2_off = -1;
				req2_size = 0;
				consumed_req = true;
			}
		}
		if (!m_b.m_inprogress) {
			if (CouldUseAligned(req2_off, m_b.m_off)) {
				if (m_b.m_failed) {
					m_b.m_failed = false;
					m_b.m_off = -1;
					m_errors.fetch_add(1, std::memory_order_relaxed);
					return -1;
				}
				m_b.OverlapCopy(req2_off, req2_size, buffer + req2_off - offset,
								false);
				req2_off = -1;
				req2_size = 0;
				consumed_req = true;
			}
			if (CouldUseAligned(req1_off, m_b.m_off)) {
				if (m_b.m_failed) {
					m_b.m_failed = false;
					m_b.m_off = -1;
					m_errors.fetch_add(1, std::memory_order_relaxed);
					return -1;
				}
				m_b.OverlapCopy(req1_off, req1_size, buffer + req1_off - offset,
								false);
				req1_off = req2_off;
				req1_size = req2_size;
				req2_off = -1;
				req2_size = 0;
				consumed_req = true;
			}
		}
		if (consumed_req) {
			continue;
		}

		// No caches serve our requests - we must kick off a new download
		// std::cout << "Will download data via cache; req1 offset=" << req1_off
		// << ", req2 offset=" << req2_off << "\n";
		bool download_a = false, download_b = false, prefetch_b = false;
		if (!m_a.m_inprogress && m_b.m_inprogress) {
			m_a.m_off = req1_off / m_cache_entry_size * m_cache_entry_size;
			m_a.m_inprogress = true;
			download_a = true;
		} else if (m_a.m_inprogress && !m_b.m_inprogress) {
			m_b.m_off = req1_off / m_cache_entry_size * m_cache_entry_size;
			m_b.m_inprogress = true;
			download_b = true;
		} else if (!m_a.m_inprogress && !m_b.m_inprogress) {
			if (req2_off != -1) {
				m_a.m_off = req1_off / m_cache_entry_size * m_cache_entry_size;
				m_a.m_inprogress = true;
				download_a = true;
				m_b.m_off = req2_off / m_cache_entry_size * m_cache_entry_size;
				m_b.m_inprogress = true;
				download_b = true;
			} else {
				if (m_a.m_used >= m_cache_entry_size) {
					// Cache A is fully read -- let's empty it
					m_a.m_off = m_b.m_off;
					m_b.m_off = -1;
					m_a.m_used = m_b.m_used;
					m_b.m_used = 0;
					std::swap(m_a.m_data, m_b.m_data);
				}
				if (m_a.m_used >= m_cache_entry_size) {
					// Both caches were fully read -- empty the second one.
					m_a.m_off = -1;
					m_a.m_used = 0;
				}
				if (m_a.m_off == -1 && m_b.m_off == -1) {
					// Prefetch both caches at once
					m_a.m_off = req1_off /
								static_cast<off_t>(m_cache_entry_size) *
								static_cast<off_t>(m_cache_entry_size);
					auto prefetch_offset =
						m_a.m_off + static_cast<off_t>(m_cache_entry_size);
					;
					download_a = true;
					m_a.m_inprogress = true;
					if (prefetch_offset < m_parent.content_length) {
						m_b.m_off = prefetch_offset;
						prefetch_b = true;
						m_b.m_inprogress = true;
					}
				} else {
					// Select one cache entry to fetch data.
					auto needed_off = req1_off /
									  static_cast<off_t>(m_cache_entry_size) *
									  static_cast<off_t>(m_cache_entry_size);
					if (needed_off > m_a.m_off) {
						m_b.m_off = needed_off;
						download_b = true;
						m_b.m_inprogress = true;
						auto bytes_unused =
							static_cast<ssize_t>(m_cache_entry_size) -
							static_cast<ssize_t>(m_b.m_used);
						m_unused_bytes.fetch_add(
							bytes_unused < 0 ? 0 : bytes_unused,
							std::memory_order_relaxed);
					} else {
						m_a.m_off = needed_off;
						download_a = true;
						m_a.m_inprogress = true;
						auto bytes_unused =
							static_cast<ssize_t>(m_cache_entry_size) -
							static_cast<ssize_t>(m_a.m_used);
						m_unused_bytes.fetch_add(
							bytes_unused < 0 ? 0 : bytes_unused,
							std::memory_order_relaxed);
					}
				}
			}
		} // else both caches are in-progress and neither satisfied our needs
		if (download_a) {
			size_t request_size = m_cache_entry_size;
			if (m_a.m_off + static_cast<off_t>(request_size) >
				m_parent.content_length) {
				request_size = m_parent.content_length - m_a.m_off;
			}
			m_fetch_count.fetch_add(1, std::memory_order_relaxed);
			m_fetch_bytes.fetch_add(request_size, std::memory_order_relaxed);
		}
		if (download_b) {
			size_t request_size = m_cache_entry_size;
			if (m_b.m_off + static_cast<off_t>(request_size) >
				m_parent.content_length) {
				request_size = m_parent.content_length - m_b.m_off;
			}
			m_fetch_count.fetch_add(1, std::memory_order_relaxed);
			m_fetch_bytes.fetch_add(request_size, std::memory_order_relaxed);
		}
		if (prefetch_b) {
			size_t request_size = m_cache_entry_size;
			if (m_b.m_off + static_cast<off_t>(request_size) >
				m_parent.content_length) {
				request_size = m_parent.content_length - m_b.m_off;
			}
			m_prefetch_count.fetch_add(1, std::memory_order_relaxed);
			m_prefetch_bytes.fetch_add(request_size, std::memory_order_relaxed);
		}
		DownloadCaches(download_a, download_b || prefetch_b, true);
	}
	return size;
}

void S3File::Shutdown() {
	std::unique_lock lock(m_shutdown_lock);
	m_shutdown_requested = true;
	m_shutdown_requested_cv.notify_one();

	m_shutdown_complete_cv.wait(lock, [] { return m_shutdown_complete; });
}

void S3File::S3Cache::Entry::Notify() {
	std::unique_lock lk(m_parent.m_mutex);
	m_inprogress = false;
	m_failed = !m_request->getErrorCode().empty();
	auto duration = m_request->getElapsedTime();
	m_parent.m_fetch_duration.fetch_add(duration.count(),
										std::memory_order_relaxed);
	if ((m_parent.m_parent.m_log.getMsgMask() & LogMask::Warning) && m_failed) {
		std::stringstream ss;
		auto duration_ms =
			std::chrono::duration_cast<std::chrono::milliseconds>(duration)
				.count();
		ss << "Finished GET for object=" << m_parent.m_parent.m_object
		   << ", offset=" << m_off << ", size=" << m_data.size()
		   << ", duration_ms=" << duration_ms << "; failed with error '"
		   << m_request->getErrorCode() << "'";
		m_parent.m_parent.m_log.Log(LogMask::Warning, "cache",
									ss.str().c_str());
	} else if (m_parent.m_parent.m_log.getMsgMask() & LogMask::Debug) {
		std::stringstream ss;
		auto duration_ms =
			std::chrono::duration_cast<std::chrono::milliseconds>(duration)
				.count();
		ss << "Finished GET for object=" << m_parent.m_parent.m_object
		   << ", offset=" << m_off << ", size=" << m_data.size()
		   << ", duration_ms=" << duration_ms << "; succeeded";
		m_parent.m_parent.m_log.Log(LogMask::Debug, "cache", ss.str().c_str());
	}
	m_request = nullptr;

	m_parent.m_cv.notify_all();
}

void S3File::S3Cache::Entry::Download(S3File &file) {
	m_used = false;
	size_t request_size = m_cache_entry_size;
	if (m_off + static_cast<off_t>(request_size) > file.content_length) {
		request_size = file.content_length - m_off;
	}
	m_data.resize(request_size);
	m_request.reset(new AmazonS3NonblockingDownload<Entry>(
		file.m_ai, file.m_object, file.m_log, m_data.data(), *this));
	// This function is always called with m_mutex held; however,
	// SendRequest can block if the threads are all busy; the threads
	// will need to grab the lock to notify of completion.  So, we
	// must release the lock here before calling a blocking function --
	// otherwise deadlock may occur.
	auto off = m_off;
	m_parent.m_mutex.unlock();

	if (file.m_log.getMsgMask() & LogMask::Debug) {
		std::stringstream ss;
		ss << "Issuing GET for object=" << file.m_object << ", offset=" << m_off
		   << ", size=" << request_size;
		file.m_log.Log(LogMask::Debug, "cache", ss.str().c_str());
	}

	if (!m_request->SendRequest(off, request_size)) {
		m_parent.m_mutex.lock();
		std::stringstream ss;
		ss << "Failed to send GetObject command: "
		   << m_request->getResponseCode() << "'"
		   << m_request->getResultString() << "'";
		file.m_log.Log(LogMask::Warning, "S3File::Read", ss.str().c_str());
		m_failed = true;
		m_request.reset();
	} else {
		m_parent.m_mutex.lock();
	}
}

extern "C" {

/*
	This function is called when we are wrapping something.
*/
XrdOss *XrdOssAddStorageSystem2(XrdOss *curr_oss, XrdSysLogger *Logger,
								const char *config_fn, const char *parms,
								XrdOucEnv *envP) {
	XrdSysError log(Logger, "s3_");

	log.Emsg("Initialize",
			 "S3 filesystem cannot be stacked with other filesystems");
	return nullptr;
}

/*
	This function is called when it is the top level file system and we are not
	wrapping anything
*/
XrdOss *XrdOssGetStorageSystem2(XrdOss *native_oss, XrdSysLogger *Logger,
								const char *config_fn, const char *parms,
								XrdOucEnv *envP) {
	auto log = new XrdSysError(Logger, "s3_");

	envP->Export("XRDXROOTD_NOPOSC", "1");

	S3File::LaunchMonitorThread(*log, envP);
	try {
		AmazonRequest::Init(*log);
		g_s3_oss = new S3FileSystem(Logger, config_fn, envP);
		return g_s3_oss;
	} catch (std::runtime_error &re) {
		log->Emsg("Initialize", "Encountered a runtime failure", re.what());
		return nullptr;
	}
}

XrdOss *XrdOssGetStorageSystem(XrdOss *native_oss, XrdSysLogger *Logger,
							   const char *config_fn, const char *parms) {
	return XrdOssGetStorageSystem2(native_oss, Logger, config_fn, parms,
								   nullptr);
}

} // end extern "C"

XrdVERSIONINFO(XrdOssGetStorageSystem, s3);
XrdVERSIONINFO(XrdOssGetStorageSystem2, s3);
XrdVERSIONINFO(XrdOssAddStorageSystem2, s3);
