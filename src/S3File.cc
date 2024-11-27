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

#include <curl/curl.h>

#include <algorithm>
#include <charconv>
#include <filesystem>
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

XrdVERSIONINFO(XrdOssGetFileSystem, S3);

std::vector<std::pair<std::weak_ptr<std::mutex>,
					  std::weak_ptr<AmazonS3SendMultipartPart>>>
	S3File::m_pending_ops;
std::mutex S3File::m_pending_lk;
std::once_flag S3File::m_monitor_launch;

S3File::S3File(XrdSysError &log, S3FileSystem *oss)
	: m_log(log), m_oss(oss), content_length(0), last_modified(0),
	  partNumber(1) {}

int S3File::Open(const char *path, int Oflag, mode_t Mode, XrdOucEnv &env) {
	if (Oflag & O_CREAT) {
		m_log.Log(LogMask::Info, "Open", "File opened for creation:", path);
		m_create = true;
	}
	if (Oflag & O_APPEND) {
		m_log.Log(LogMask::Info, "Open", "File opened for append:", path);
	}
	if (Oflag & (O_RDWR | O_WRONLY)) {
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

	m_ai = *ai;
	m_object = object;

	// This flag is not set when it's going to be a read operation
	// so we check if the file exists in order to be able to return a 404
	if (!Oflag || (Oflag & O_APPEND)) {
		AmazonS3Head head(m_ai, m_object, m_log);

		if (!head.SendRequest()) {
			return -ENOENT;
		}
		head.getSize();
	}

	return 0;
}

ssize_t S3File::Read(void *buffer, off_t offset, size_t size) {
	AmazonS3Download download(m_ai, m_object, m_log);

	if (!download.SendRequest(offset, size)) {
		std::stringstream ss;
		ss << "Failed to send GetObject command: " << download.getResponseCode()
		   << "'" << download.getResultString() << "'";
		m_log.Log(LogMask::Warning, "S3File::Read", ss.str().c_str());
		return 0;
	}

	const std::string &bytes = download.getResultString();
	memcpy(buffer, bytes.data(), bytes.size());
	return bytes.size();
}

int S3File::Fstat(struct stat *buff) {
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
			ss << "Failed to send HEAD command: " << head.getErrorCode() << ": "
			   << head.getErrorMessage();
			m_log.Log(LogMask::Warning, "S3File::Fstat", ss.str().c_str());
			return -EIO;
		}
	}

	std::string headers = head.getResultString();

	std::string line;
	size_t current_newline = 0;
	size_t next_newline = std::string::npos;
	size_t last_character = headers.size();
	while (current_newline != std::string::npos &&
		   current_newline != last_character - 1) {
		next_newline = headers.find("\r\n", current_newline + 2);
		line = substring(headers, current_newline + 2, next_newline);

		size_t colon = line.find(":");
		if (colon != std::string::npos && colon != line.size()) {
			std::string attr = substring(line, 0, colon);
			std::string value = substring(line, colon + 1);
			trim(value);
			toLower(attr);

			if (attr == "content-length") {
				this->content_length = std::stol(value);
			} else if (attr == "last-modified") {
				struct tm t;
				char *eos = strptime(value.c_str(), "%a, %d %b %Y %T %Z", &t);
				if (eos == &value.c_str()[value.size()]) {
					time_t epoch = timegm(&t);
					if (epoch != -1) {
						this->last_modified = epoch;
					}
				}
			}
		}

		current_newline = next_newline;
	}

	memset(buff, '\0', sizeof(struct stat));
	buff->st_mode = 0600 | S_IFREG;
	buff->st_nlink = 1;
	buff->st_uid = 1;
	buff->st_gid = 1;
	buff->st_size = this->content_length;
	buff->st_mtime = this->last_modified;
	buff->st_atime = 0;
	buff->st_ctime = 0;
	buff->st_dev = 0;
	buff->st_ino = 0;

	return 0;
}

ssize_t S3File::Write(const void *buffer, off_t offset, size_t size) {
	auto write_mutex = m_write_lk;
	if (!write_mutex) {
		return -EBADF;
	}
	std::lock_guard lk(*write_mutex);

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

ssize_t S3File::ContinueSendPart(const void *buffer, size_t size) {
	m_part_written += size;
	auto write_size = size;
	if (m_part_written > m_s3_part_size) {
		write_size = size - (m_part_written - m_s3_part_size);
		m_part_written = m_s3_part_size;
	}
	auto is_final = (m_part_size > 0 && m_part_written == m_part_size) ||
					m_part_written == m_s3_part_size;
	if (m_log.getMsgMask() & LogMask::Dump) {
		std::stringstream ss;
		ss << "Sending request with buffer of size=" << write_size
		   << " and is_final=" << is_final;
		m_log.Log(LogMask::Dump, "ContinueSendPart", ss.str().c_str());
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
		auto &resultString = m_write_op->getResultString();
		std::size_t startPos = resultString.find("ETag:");
		if (startPos == std::string::npos) {
			m_log.Emsg("Write", "Result from S3 does not include ETag:",
					   resultString.c_str());
			m_write_op.reset();
			m_write_offset = -1;
			return -EIO;
		}
		std::size_t endPos = resultString.find("\"", startPos + 7);
		if (startPos == std::string::npos) {
			m_log.Emsg("Write",
					   "Result from S3 does not include ETag end-character:",
					   resultString.c_str());
			m_write_op.reset();
			m_write_offset = -1;
			return -EIO;
		}
		eTags.push_back(
			resultString.substr(startPos + 7, endPos - startPos - 7));
		m_write_op.reset();
		partNumber++;
	}

	return write_size;
}

void S3File::LaunchMonitorThread() {
	std::call_once(m_monitor_launch, [] {
		std::thread t(S3File::CleanupTransfers);
		t.detach();
	});
}

void S3File::CleanupTransfers() {
	while (true) {
		std::this_thread::sleep_for(HTTPRequest::GetStallTimeout() / 3);
		try {
			CleanupTransfersOnce();
		} catch (std::exception &exc) {
			std::cerr << "Warning: caught unexpected exception when trying to "
						 "clean transfers: "
					  << exc.what() << std::endl;
		}
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
	// If we opened the object in create mode but did not actually write
	// anything, make a quick zero-length file.
	if (m_create && !m_write_offset) {
		AmazonS3Upload upload(m_ai, m_object, m_log);
		if (!upload.SendRequest("")) {
			m_log.Emsg("Close", "Failed to create zero-length file");
			return -ENOENT;
		} else {
			m_log.Emsg("Open", "upload.SendRequest() succeeded");
			return 0;
		}
	}
	if (m_write_op) {
		std::lock_guard lk(*m_write_lk);
		m_part_size = m_part_written;
		auto written = ContinueSendPart(nullptr, 0);
		if (written < 0) {
			m_log.Emsg("Close", "Failed to complete the last S3 upload");
			return -ENOENT;
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

	/* Original write code
	std::string payload((char *)buffer, size);
	if (!upload.SendRequest(payload, offset, size)) {
		m_log.Emsg("Open", "upload.SendRequest() failed");
		return -ENOENT;
	} else {
		m_log.Emsg("Open", "upload.SendRequest() succeeded");
		return 0;
	} */
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

	S3File::LaunchMonitorThread();
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
