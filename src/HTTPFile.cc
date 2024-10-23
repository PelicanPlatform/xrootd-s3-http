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

#include <filesystem>
#include <iostream>
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

// Ensures that path is of the form /storagePrefix/object and returns
// the resulting object value.  The storagePrefix does not necessarily begin
// with '/'
//
// Examples:
// /foo/bar, /foo/bar/baz -> baz
// storage.com/foo, /storage.com/foo/bar -> bar
// /baz, /foo/bar -> error
int parse_path(const std::string &storagePrefixStr, const char *pathStr,
			   std::string &object) {
	const std::filesystem::path storagePath(pathStr);
	const std::filesystem::path storagePrefix(storagePrefixStr);

	auto prefixComponents = storagePrefix.begin();
	auto pathComponents = storagePath.begin();

	std::filesystem::path full;
	std::filesystem::path prefix;

	pathComponents++;
	if (!storagePrefixStr.empty() && storagePrefixStr[0] == '/') {
		prefixComponents++;
	}

	while (prefixComponents != storagePrefix.end() &&
		   *prefixComponents == *pathComponents) {
		full /= *prefixComponents++;
		prefix /= *pathComponents++;
	}

	// Check that nothing diverged before reaching end of service name
	if (prefixComponents != storagePrefix.end()) {
		return -ENOENT;
	}

	std::filesystem::path obj_path;
	while (pathComponents != storagePath.end()) {
		obj_path /= *pathComponents++;
	}

	object = obj_path.string();
	return 0;
}

int HTTPFile::Open(const char *path, int Oflag, mode_t Mode, XrdOucEnv &env) {
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
		return Fstat(&buf);
	}

	return 0;
}

ssize_t HTTPFile::Read(void *buffer, off_t offset, size_t size) {
	HTTPDownload download(m_hostUrl, m_object, m_log, m_oss->getToken());
	m_log.Log(
		LogMask::Debug, "HTTPFile::Read",
		"About to perform download from HTTPFile::Read(): hostname / object:",
		m_hostname.c_str(), m_object.c_str());

	if (!download.SendRequest(offset, size)) {
		std::stringstream ss;
		ss << "Failed to send GetObject command: " << download.getResponseCode()
		   << "'" << download.getResultString() << "'";
		m_log.Log(LogMask::Warning, "HTTPFile::Read", ss.str().c_str());
		return 0;
	}

	const std::string &bytes = download.getResultString();
	memcpy(buffer, bytes.data(), bytes.size());
	return bytes.size();
}

int HTTPFile::Fstat(struct stat *buff) {
	if (m_stat) {
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
		auto httpCode = head.getResponseCode();
		if (httpCode) {
			std::stringstream ss;
			ss << "HEAD command failed: " << head.getResponseCode() << ": "
			   << head.getResultString();
			m_log.Log(LogMask::Warning, "HTTPFile::Fstat", ss.str().c_str());
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
			m_log.Log(LogMask::Warning, "HTTPFile::Fstat", ss.str().c_str());
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

	if (buff) {
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
	}
	m_stat = true;

	return 0;
}

ssize_t HTTPFile::Write(const void *buffer, off_t offset, size_t size) {
	HTTPUpload upload(m_hostUrl, m_object, m_log, m_oss->getToken());

	std::string payload((char *)buffer, size);
	if (!upload.SendRequest(payload, offset, size)) {
		m_log.Emsg("Open", "upload.SendRequest() failed");
		return -ENOENT;
	} else {
		m_log.Emsg("Open", "upload.SendRequest() succeeded");
		return 0;
	}
}

int HTTPFile::Close(long long *retsz) {
	m_log.Emsg("Close", "Closed our HTTP file");
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
