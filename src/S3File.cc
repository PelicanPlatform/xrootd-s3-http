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

#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

using namespace XrdHTTPServer;

S3FileSystem *g_s3_oss = nullptr;

XrdVERSIONINFO(XrdOssGetFileSystem, S3);

S3File::S3File(XrdSysError &log, S3FileSystem *oss)
	: m_log(log), m_oss(oss), content_length(0), last_modified(0) {}

int parse_path(const S3FileSystem &fs, const char *fullPath,
			   std::string &exposedPath, std::string &object) {
	//
	// Check the path for validity.
	//
	std::filesystem::path p(fullPath);
	auto pathComponents = p.begin();

	// Iterate through components of the fullPath until we either find a match
	// or we've reached the end of the path.
	std::filesystem::path currentPath = *pathComponents;
	while (pathComponents != p.end()) {
		if (fs.exposedPathExists(currentPath.string())) {
			exposedPath = currentPath.string();
			break;
		}
		++pathComponents;
		if (pathComponents != p.end()) {
			currentPath /= *pathComponents;
		} else {
			return -ENOENT;
		}
	}

	// Objects names may contain path separators.
	++pathComponents;
	if (pathComponents == p.end()) {
		return -ENOENT;
	}

	std::filesystem::path objectPath = *pathComponents++;
	for (; pathComponents != p.end(); ++pathComponents) {
		objectPath /= (*pathComponents);
	}
	object = objectPath.string();

	fprintf(stderr, "object = %s\n", object.c_str());

	return 0;
}

int S3File::Open(const char *path, int Oflag, mode_t Mode, XrdOucEnv &env) {
	std::string exposedPath, object;
	int rv = parse_path(*m_oss, path, exposedPath, object);
	if (rv != 0) {
		return rv;
	}
	if (!m_oss->exposedPathExists(exposedPath))
		return -ENOENT;

	std::string configured_s3_region = m_oss->getS3Region(exposedPath);
	std::string configured_s3_service_url = m_oss->getS3ServiceURL(exposedPath);
	std::string configured_s3_access_key =
		m_oss->getS3AccessKeyFile(exposedPath);
	std::string configured_s3_secret_key =
		m_oss->getS3SecretKeyFile(exposedPath);
	std::string configured_s3_bucket_name = m_oss->getS3BucketName(exposedPath);
	std::string configured_s3_url_style = m_oss->getS3URLStyle();

	// We used to query S3 here to see if the object existed, but of course
	// if you're creating a file on upload, you don't care.

	this->s3_object_name = object;
	this->s3_bucket_name = configured_s3_bucket_name;
	this->s3_service_url = configured_s3_service_url;
	this->s3_access_key = configured_s3_access_key;
	this->s3_secret_key = configured_s3_secret_key;
	this->s3_url_style = configured_s3_url_style;

	// This flag is not set when it's going to be a read operation
	// so we check if the file exists in order to be able to return a 404
	if (!Oflag) {
		AmazonS3Head head(this->s3_service_url, this->s3_access_key,
						  this->s3_secret_key, this->s3_bucket_name,
						  this->s3_object_name, this->s3_url_style, m_log);

		if (!head.SendRequest()) {
			return -ENOENT;
		}
	}

	return 0;
}

ssize_t S3File::Read(void *buffer, off_t offset, size_t size) {
	AmazonS3Download download(this->s3_service_url, this->s3_access_key,
							  this->s3_secret_key, this->s3_bucket_name,
							  this->s3_object_name, this->s3_url_style, m_log);

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
	AmazonS3Head head(this->s3_service_url, this->s3_access_key,
					  this->s3_secret_key, this->s3_bucket_name,
					  this->s3_object_name, this->s3_url_style, m_log);

	if (!head.SendRequest()) {
		// SendRequest() returns false for all errors, including ones
		// where the server properly responded with something other
		// than code 200.  If xrootd wants us to distinguish between
		// these cases, head.getResponseCode() is initialized to 0, so
		// we can check.
		std::stringstream ss;
		ss << "Failed to send HeadObject command: " << head.getResponseCode()
		   << "'" << head.getResultString() << "'";
		m_log.Log(LogMask::Warning, "S3File::Fstat", ss.str().c_str());
		return -ENOENT;
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
	AmazonS3Upload upload(this->s3_service_url, this->s3_access_key,
						  this->s3_secret_key, this->s3_bucket_name,
						  this->s3_object_name, this->s3_url_style, m_log);

	std::string payload((char *)buffer, size);
	if (!upload.SendRequest(payload, offset, size)) {
		m_log.Emsg("Open", "upload.SendRequest() failed");
		return -ENOENT;
	} else {
		m_log.Emsg("Open", "upload.SendRequest() succeeded");
		return 0;
	}
}

int S3File::Close(long long *retsz) {
	m_log.Emsg("Close", "Closed our S3 file");
	return 0;
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
	XrdSysError log(Logger, "s3_");

	envP->Export("XRDXROOTD_NOPOSC", "1");

	try {
		g_s3_oss = new S3FileSystem(Logger, config_fn, envP);
		return g_s3_oss;
	} catch (std::runtime_error &re) {
		log.Emsg("Initialize", "Encountered a runtime failure", re.what());
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
