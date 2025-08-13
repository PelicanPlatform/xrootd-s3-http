/***************************************************************
 *
 * Copyright (C) 2025, Pelican Project, Morgridge Institute for Research
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

#include "HTTPFile.hh"
#include "HTTPCommands.hh"
#include "HTTPFileSystem.hh"
#include "logging.hh"
#include "stl_string_utils.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucStream.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSec/XrdSecEntityAttr.hh>
#include <XrdSfs/XrdSfsInterface.hh>
#include <XrdVersion.hh>
#include <curl/curl.h>

#include <charconv>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

using namespace XrdHTTPServer;

HTTPFileSystem *g_http_oss = nullptr;

XrdVERSIONINFO(XrdOssGetFileSystem, HTTP);

HTTPFile::HTTPFile(XrdSysError &log, HTTPFileSystem *oss)
	: m_log(log), m_oss(oss), content_length(0), last_modified(0) {}

int HTTPFile::Open(const char *path, int Oflag, mode_t Mode, XrdOucEnv &env) {
	if (m_is_open) {
		m_log.Log(LogMask::Warning, "HTTPFile::Open",
				  "File already open:", path);
		return -EBADF;
	}
	if (Oflag & O_CREAT) {
		m_log.Log(LogMask::Info, "HTTPFile::Open",
				  "File opened for creation:", path);
	}
	if (Oflag & O_APPEND) {
		m_log.Log(LogMask::Info, "HTTPFile::Open",
				  "File opened for append:", path);
	}
	if (Oflag & (O_RDWR | O_WRONLY)) {
		m_write = true;
		m_log.Log(LogMask::Debug, "HTTPFile::Open",
				  "File opened for writing:", path);
		m_write_lk.reset(new std::mutex);
	}
	// get the expected file size; only relevant for O_RDWR | O_WRONLY
	char *asize_char;
	if ((asize_char = env.Get("oss.asize"))) {
		off_t result{0};
		auto [ptr, ec] = std::from_chars(
			asize_char, asize_char + strlen(asize_char), result);
		if (ec == std::errc() && ptr == asize_char + strlen(asize_char)) {
			if (result < 0) {
				m_log.Log(LogMask::Warning, "HTTPFile::Open",
						  "Opened file has oss.asize set to a negative value:",
						  asize_char);
				return -EIO;
			}
			m_object_size = result;
		} else {
			std::stringstream ss;
			ss << "Opened file has oss.asize set to an unparseable value: "
			   << asize_char;
			m_log.Log(LogMask::Warning, "HTTPFile::Open", ss.str().c_str());
			return -EIO;
		}
	}

	auto configured_hostname = m_oss->getHTTPHostName();
	auto configured_hostUrl = m_oss->getHTTPHostUrl();
	const auto &configured_url_base = m_oss->getHTTPUrlBase();
	if (!configured_url_base.empty()) {
		configured_hostUrl = configured_url_base;
		configured_hostname = m_oss->getStoragePrefix();
	}

	//
	// Check the path for validity.
	//
	std::string object;
	int rv = parse_path(configured_hostname, path, object);

	if (rv != 0) {
		return rv;
	}

	m_object = object;
	m_hostname = configured_hostname;
	m_hostUrl = configured_hostUrl;

	if (!Oflag) {
		struct stat buf;
		auto rv = Fstat(&buf);
		if (rv < 0) {
			return rv;
		}
		if (S_ISDIR(buf.st_mode)) {
			return -EISDIR;
		} else {
			return 0;
		}
		// XXX May need to return an error here to show that the request is
		// against a directory instead of file could be:
		// https://man7.org/linux/man-pages/man2/open.2.html return may be
		// -EISDIR
	}

	m_is_open = true;
	return 0;
}

ssize_t HTTPFile::Read(void *buffer, off_t offset, size_t size) {
	if (!m_is_open) {
		m_log.Log(LogMask::Warning, "HTTPFile::Read", "File not open");
		return -EBADF;
	}
	HTTPDownload download(m_hostUrl, m_object, m_log, m_oss->getToken());
	m_log.Log(
		LogMask::Debug, "HTTPFile::Read",
		"About to perform download from HTTPFile::Read(): hostname / object:",
		m_hostname.c_str(), m_object.c_str());

	if (!download.SendRequest(offset, size)) {
		return HTTPRequest::HandleHTTPError(download, m_log, "GET",
											m_object.c_str());
	}

	const std::string &bytes = download.getResultString();
	memcpy(buffer, bytes.data(), bytes.size());
	return bytes.size();
}

int HTTPFile::Fstat(struct stat *buff) {
	if (m_stat) {
		memset(buff, '\0', sizeof(struct stat));
		if (m_object == "")
			buff->st_mode = 0600 | S_IFDIR;
		else
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
		return 0;
	}

	m_log.Log(LogMask::Debug, "HTTPFile::Fstat",
			  "About to perform HTTPFile::Fstat():", m_hostUrl.c_str(),
			  m_object.c_str());
	HTTPHead head(m_hostUrl, m_object, m_log, m_oss->getToken());

	if (!head.SendRequest()) {
		// SendRequest() returns false for all errors, including ones
		// where the server properly responded with something other
		// than code 200.  If xrootd wants us to distinguish between
		// these cases, head.getResponseCode() is initialized to 0, so
		// we can check.
		return HTTPRequest::HandleHTTPError(head, m_log, "HEAD",
											m_object.c_str());
	}

	std::string headers = head.getResultString();

	std::string line;
	size_t current_newline = 0;
	size_t next_newline = std::string::npos;
	size_t last_character = headers.size();
	while (current_newline != std::string::npos &&
		   current_newline != last_character - 1 && last_character) {
		next_newline = headers.find("\r\n", current_newline + 2);
		std::string line =
			substring(headers, current_newline + 2, next_newline);

		size_t colon = line.find(":");
		if (colon != std::string::npos && colon != line.size()) {
			std::string attr = substring(line, 0, colon);
			toLower(attr); // Some servers might not follow conventional
						   // capitalization schemes
			std::string value = substring(line, colon + 1);
			trim(value);

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

	// headers are totally different for a file versus an html stream
	// describing a directory. note that here and fill out the buffer
	// accordingly

	buff->st_mode = 0600 | S_IFDIR;
	if (buff) {
		memset(buff, '\0', sizeof(struct stat));
		if (m_object == "" || m_object.back() == '/')
			buff->st_mode = 0600 | S_IFDIR;
		else
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
	}
	m_stat = true;

	return 0;
}

ssize_t HTTPFile::Write(const void *buffer, off_t offset, size_t size) {
	if (!m_is_open) {
		m_log.Log(LogMask::Warning, "HTTPFile::Write", "File not open");
		return -EBADF;
	}

	if (!m_write_lk) {
		return -EBADF;
	}
	std::lock_guard lk(*m_write_lk);

	// Small object optimization as in S3File::Write()
	if (!m_write_offset && m_object_size == static_cast<off_t>(size)) {
		HTTPUpload upload(m_hostUrl, m_object, m_log, m_oss->getToken());
		std::string payload((char *)buffer, size);
		if (!upload.SendRequest(payload)) {
			return HTTPRequest::HandleHTTPError(upload, m_log, "PUT",
												m_object.c_str());
		} else {
			m_write_offset += size;
			m_log.Log(LogMask::Debug, "HTTPFile::Write",
					  "Creation of small object succeeded",
					  std::to_string(size).c_str());
			return size;
		}
	}
	// If we don't have an in-progress upload, start one
	if (!m_write_op) {
		if (offset != 0) {
			m_log.Log(LogMask::Error, "HTTPFile::Write",
					  "Out-of-order write detected; HTTP "
					  "requires writes to be in order");
			m_write_offset = -1;
			return -EIO;
		}
		m_write_op.reset(
			new HTTPUpload(m_hostUrl, m_object, m_log, m_oss->getToken()));
		std::string payload((char *)buffer, size);
		if (!m_write_op->StartStreamingRequest(payload, m_object_size)) {
			return HTTPRequest::HandleHTTPError(
				*m_write_op, m_log, "PUT streaming start", m_object.c_str());
		} else {
			m_write_offset += size;
			m_log.Log(LogMask::Debug, "HTTPFile::Write",
					  "First write request succeeded",
					  std::to_string(size).c_str());
			return size;
		}
	}
	// Validate continuing writing at offset
	if (offset != static_cast<int64_t>(m_write_offset)) {
		std::stringstream ss;
		ss << "Requested write offset at " << offset
		   << " does not match current file descriptor offset at "
		   << m_write_offset;
		m_log.Log(LogMask::Warning, "HTTPFile::Write", ss.str().c_str());
		return -EIO;
	}

	// Continue the write
	std::string payload((char *)buffer, size);
	if (!m_write_op->ContinueStreamingRequest(payload, m_object_size, false)) {
		return HTTPRequest::HandleHTTPError(
			*m_write_op, m_log, "PUT streaming continue", m_object.c_str());
	} else {
		m_write_offset += size;
		m_log.Log(LogMask::Debug, "HTTPFile::Write",
				  "Continued request succeeded", std::to_string(size).c_str());
	}
	return size;
}

int HTTPFile::Close(long long *retsz) {
	if (!m_is_open) {
		m_log.Log(LogMask::Error, "HTTPFile::Close",
				  "Cannot close. URL isn't open");
		return -EBADF;
	}
	m_is_open = false;
	// If we opened the object in write mode but did not actually write
	// anything, make a quick zero-length file.
	if (m_write && !m_write_offset) {
		HTTPUpload upload(m_hostUrl, m_object, m_log, m_oss->getToken());
		if (!upload.SendRequest("")) {
			return HTTPRequest::HandleHTTPError(
				upload, m_log, "PUT zero-length", m_object.c_str());
		} else {
			m_log.Log(LogMask::Debug, "HTTPFile::Close",
					  "Creation of zero-length succeeded");
			return 0;
		}
	}

	if (m_write && m_object_size == -1) {
		// if we didn't get a size, we need to explicitly close the upload
		if (!m_write_op->ContinueStreamingRequest("", 0, true)) {
			return HTTPRequest::HandleHTTPError(
				*m_write_op, m_log, "PUT streaming close", m_object.c_str());
		} else {
			m_log.Log(LogMask::Debug, "HTTPFile::Write",
					  "PUT streaming close succeeded");
		}
	}

	m_log.Log(LogMask::Debug, "HTTPFile::Close",
			  "Closed HTTP file:", m_object.c_str());
	return 0;
}

extern "C" {

/*
	This function is called when we are wrapping something.
*/
XrdOss *XrdOssAddStorageSystem2(XrdOss *curr_oss, XrdSysLogger *Logger,
								const char *config_fn, const char *parms,
								XrdOucEnv *envP) {
	XrdSysError log(Logger, "httpserver_");

	log.Emsg("Initialize",
			 "HTTP filesystem cannot be stacked with other filesystems");
	return nullptr;
}

/*
	This function is called when it is the top level file system and we are not
	wrapping anything
*/
XrdOss *XrdOssGetStorageSystem2(XrdOss *native_oss, XrdSysLogger *Logger,
								const char *config_fn, const char *parms,
								XrdOucEnv *envP) {
	auto log = new XrdSysError(Logger, "httpserver_");

	envP->Export("XRDXROOTD_NOPOSC", "1");

	log->Log(LogMask::Debug, "XrdOssGetStorageSystem2", "called");

	try {
		HTTPRequest::Init(*log);
		g_http_oss = new HTTPFileSystem(Logger, config_fn, envP);
		return g_http_oss;
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

XrdVERSIONINFO(XrdOssGetStorageSystem, HTTPserver);
XrdVERSIONINFO(XrdOssGetStorageSystem2, HTTPserver);
XrdVERSIONINFO(XrdOssAddStorageSystem2, HTTPserver);
