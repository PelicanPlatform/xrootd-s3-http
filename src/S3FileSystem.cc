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

#include "S3FileSystem.hh"
#include "S3AccessInfo.hh"
#include "S3Directory.hh"
#include "S3File.hh"
#include "logging.hh"
#include "shortfile.hh"
#include "stl_string_utils.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucGatherConf.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdVersion.hh>

#include <algorithm>
#include <charconv>
#include <filesystem>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

bool S3FileSystem::m_dir_marker = true;
std::string S3FileSystem::m_dir_marker_name = ".pelican_dir_marker";

S3FileSystem::S3FileSystem(XrdSysLogger *lp, const char *configfn,
						   XrdOucEnv * /*envP*/)
	: m_log(lp, "s3_") {
	m_log.Say("------ Initializing the S3 filesystem plugin.");
	if (!Config(lp, configfn)) {
		throw std::runtime_error("Failed to configure S3 filesystem plugin.");
	}
}

S3FileSystem::~S3FileSystem() {}

bool S3FileSystem::handle_required_config(const char *desired_name,
										  const std::string &source) {
	if (source.empty()) {
		std::string error;
		formatstr(error, "%s must specify a value", desired_name);
		m_log.Emsg("Config", error.c_str());
		return false;
	}
	return true;
}

bool S3FileSystem::Config(XrdSysLogger *lp, const char *configfn) {
	XrdOucEnv myEnv;
	XrdOucGatherConf s3server_conf("s3.", &m_log);
	int result;
	if ((result = s3server_conf.Gather(configfn,
									   XrdOucGatherConf::full_lines)) < 0) {
		m_log.Emsg("Config", -result, "parsing config file", configfn);
		return false;
	}

	char *temporary;
	std::string value;
	std::string attribute;
	std::shared_ptr<S3AccessInfo> newAccessInfo(new S3AccessInfo());
	std::string exposedPath;
	m_log.setMsgMask(0);
	while ((temporary = s3server_conf.GetLine())) {
		attribute = s3server_conf.GetToken();
		if (attribute == "s3.trace") {
			if (!XrdHTTPServer::ConfigLog(s3server_conf, m_log)) {
				m_log.Emsg("Config", "Failed to configure the log level");
			}
			continue;
		} else if (attribute == "s3.cache_entry_size") {
			size_t size;
			auto value = s3server_conf.GetToken();
			if (!value) {
				m_log.Emsg("Config", "s3.cache_entry_size must be specified");
				return false;
			}
			std::string_view value_sv(value);
			auto result = std::from_chars(
				value_sv.data(), value_sv.data() + value_sv.size(), size);
			if (result.ec != std::errc()) {
				m_log.Emsg("Config", "s3.cache_entry_size must be a number");
				return false;
			} else if (result.ptr != value_sv.data() + value_sv.size()) {
				m_log.Emsg("Config",
						   "s3.cache_entry_size contains trailing characters");
				return false;
			}
			S3File::SetCacheEntrySize(size);
			continue;
		}

		temporary = s3server_conf.GetToken();
		if (attribute == "s3.end") {
			m_s3_access_map[exposedPath] = newAccessInfo;
			if (newAccessInfo->getS3ServiceName().empty()) {
				m_log.Emsg("Config", "s3.service_name not specified");
				return false;
			}
			if (newAccessInfo->getS3Region().empty()) {
				m_log.Emsg("Config", "s3.region not specified");
				return false;
			}
			std::string contents;
			if (newAccessInfo->getS3AccessKeyFile() != "") {
				if (!readShortFile(newAccessInfo->getS3AccessKeyFile(),
								   contents)) {
					m_log.Emsg("Config", "s3.access_key_file not readable");
					return false;
				}
			}
			if (newAccessInfo->getS3SecretKeyFile() != "") {
				if (!readShortFile(newAccessInfo->getS3SecretKeyFile(),
								   contents)) {
					m_log.Emsg("Config", "s3.secret_key_file not readable");
					return false;
				}
			}
			newAccessInfo.reset(new S3AccessInfo());
			exposedPath = "";
			continue;
		}
		if (!temporary) {
			continue;
		}
		value = temporary;

		if (!handle_required_config("s3.path_name", value)) {
			return false;
		}
		if (!handle_required_config("s3.bucket_name", value)) {
			return false;
		}
		if (!handle_required_config("s3.service_name", value)) {
			return false;
		}
		if (!handle_required_config("s3.region", value)) {
			return false;
		}
		if (!handle_required_config("s3.service_url", value)) {
			return false;
		}
		if (!handle_required_config("s3.access_key_file", value)) {
			return false;
		}
		if (!handle_required_config("s3.secret_key_file", value)) {
			return false;
		}
		if (!handle_required_config("s3.url_style", value)) {
			return false;
		}

		if (attribute == "s3.path_name") {
			// Normalize paths so that they all start with /
			if (value[0] != '/') {
				exposedPath = "/" + value;
			} else {
				exposedPath = value;
			}
		} else if (attribute == "s3.bucket_name")
			newAccessInfo->setS3BucketName(value);
		else if (attribute == "s3.service_name")
			newAccessInfo->setS3ServiceName(value);
		else if (attribute == "s3.region")
			newAccessInfo->setS3Region(value);
		else if (attribute == "s3.access_key_file")
			newAccessInfo->setS3AccessKeyFile(value);
		else if (attribute == "s3.secret_key_file")
			newAccessInfo->setS3SecretKeyFile(value);
		else if (attribute == "s3.service_url")
			newAccessInfo->setS3ServiceUrl(value);
		else if (attribute == "s3.url_style") {
			s3_url_style = value;
			newAccessInfo->setS3UrlStyle(s3_url_style);
		}
	}

	if (s3_url_style.empty()) {
		m_log.Emsg("Config", "s3.url_style not specified");
		return false;
	} else {
		// We want this to be case-insensitive.
		toLower(s3_url_style);
	}
	if (s3_url_style != "virtual" && s3_url_style != "path") {
		m_log.Emsg(
			"Config",
			"invalid s3.url_style specified. Must be 'virtual' or 'path'");
		return false;
	}

	return true;
}

// Object Allocation Functions
//
XrdOssDF *S3FileSystem::newDir(const char *user) {
	return new S3Directory(m_log, *this);
}

XrdOssDF *S3FileSystem::newFile(const char *user) {
	return new S3File(m_log, this);
}

//
// Stat a path within the S3 bucket as if it were a hierarchical
// path.
//
// Note that S3 is _not_ a hierarchy and may contain objects that
// can't be represented inside XRootD.  In that case, we just return
// an -ENOENT.
//
// For example, consider a setup with two objects:
//
// - /foo/bar.txt
// - /foo
//
// In this case, `Stat` of `/foo` will return a file so walking the
// bucket will miss `/foo/bar.txt`
//
// We will also return an ENOENT for objects with a trailing `/`.  So,
// if there's a single object in the bucket:
//
// - /foo/bar.txt/
//
// then a `Stat` of `/foo/bar.txt` and `/foo/bar.txt/` will both return
// `-ENOENT`.
int S3FileSystem::Stat(const char *path, struct stat *buff, int opts,
					   XrdOucEnv *env) {
	m_log.Log(XrdHTTPServer::Debug, "Stat", "Stat'ing path", path);

	std::string exposedPath, object;
	auto rv = parsePath(path, exposedPath, object);
	if (rv != 0) {
		m_log.Log(XrdHTTPServer::Debug, "Stat", "Failed to parse path:", path);
		return rv;
	}
	auto ai = getS3AccessInfo(exposedPath, object);
	if (!ai) {
		m_log.Log(XrdHTTPServer::Info, "Stat",
				  "Prefix not configured for Stat");
		return -ENOENT;
	}
	if (ai->getS3BucketName().empty()) {
		return -EINVAL;
	}

	trimslashes(object);
	if (object == "") {
		if (m_dir_marker) {
			// We even do the `Stat` for `/` despite the fact we always
			// return the same directory object.  This way, we test for
			// permission denied or other errors with the S3 instance.
			object = m_dir_marker_name;
		} else {
			if (buff) {
				memset(buff, '\0', sizeof(struct stat));
				buff->st_mode = 0700 | S_IFDIR;
				buff->st_nlink = 0;
				buff->st_uid = 1;
				buff->st_gid = 1;
				buff->st_size = 4096;
				buff->st_mtime = buff->st_atime = buff->st_ctime = 0;
				buff->st_dev = 0;
				buff->st_ino = 1;
			}
			return 0;
		}
	}

	// First, check to see if the file name is an object.  If it's
	// a 404 response, then we will assume it may be a directory.
	AmazonS3Head headCommand = AmazonS3Head(*ai, object, m_log);
	auto res = headCommand.SendRequest();
	if (res) {
		if (buff) {
			memset(buff, '\0', sizeof(struct stat));
			if (object == m_dir_marker_name) {
				buff->st_mode = 0700 | S_IFDIR;
				buff->st_size = 4096;
				buff->st_nlink = 0;
			} else {
				buff->st_mode = 0600 | S_IFREG;
				buff->st_size = headCommand.getSize();
				buff->st_nlink = 1;
			}
			buff->st_uid = buff->st_gid = 1;
			buff->st_mtime = buff->st_atime = buff->st_ctime = 0;
			buff->st_dev = 0;
			buff->st_ino = 1;
		}
		return 0;
	} else {
		auto httpCode = headCommand.getResponseCode();
		if (httpCode == 0) {
			if (m_log.getMsgMask() & XrdHTTPServer::Info) {
				std::stringstream ss;
				ss << "Failed to stat path " << path
				   << "; error: " << headCommand.getErrorMessage()
				   << " (code=" << headCommand.getErrorCode() << ")";
				m_log.Log(XrdHTTPServer::Info, "Stat", ss.str().c_str());
			}
			return -EIO;
		}
		if (httpCode == 404) {
			if (object == m_dir_marker_name) {
				if (buff) {
					memset(buff, '\0', sizeof(struct stat));
					buff->st_mode = 0700 | S_IFDIR;
					buff->st_nlink = 0;
					buff->st_uid = 1;
					buff->st_gid = 1;
					buff->st_size = 4096;
					buff->st_mtime = buff->st_atime = buff->st_ctime = 0;
					buff->st_dev = 0;
					buff->st_ino = 1;
				}
				return 0;
			}
			object = object + "/";
		} else {
			if (m_log.getMsgMask() & XrdHTTPServer::Info) {
				std::stringstream ss;
				ss << "Failed to stat path " << path << "; response code "
				   << httpCode;
				m_log.Log(XrdHTTPServer::Info, "Stat", ss.str().c_str());
			}
			return httpCode == 403 ? -EACCES : -EIO;
		}
	}

	// List the object name as a pseudo-directory.  Limit the results
	// back to a single item (we're just looking to see if there's a
	// common prefix here).
	AmazonS3List listCommand(*ai, object, 1, m_log);
	res = listCommand.SendRequest("");
	if (!res) {
		auto httpCode = listCommand.getResponseCode();
		if (httpCode == 0) {
			if (m_log.getMsgMask() & XrdHTTPServer::Info) {
				std::stringstream ss;
				ss << "Failed to stat path " << path
				   << "; error: " << listCommand.getErrorMessage()
				   << " (code=" << listCommand.getErrorCode() << ")";
				m_log.Log(XrdHTTPServer::Info, "Stat", ss.str().c_str());
			}
			return -EIO;
		} else {
			if (m_log.getMsgMask() & XrdHTTPServer::Info) {
				std::stringstream ss;
				ss << "Failed to stat path " << path << "; response code "
				   << httpCode;
				m_log.Log(XrdHTTPServer::Info, "Stat", ss.str().c_str());
			}
			switch (httpCode) {
			case 404:
				return -ENOENT;
			case 403:
				return -EPERM;
			default:
				return -EIO;
			}
		}
	}

	std::string errMsg;
	std::vector<S3ObjectInfo> objInfo;
	std::vector<std::string> commonPrefixes;
	std::string ct;
	res = listCommand.Results(objInfo, commonPrefixes, ct, errMsg);
	if (!res) {
		m_log.Log(XrdHTTPServer::Warning, "Stat",
				  "Failed to parse S3 results:", errMsg.c_str());
		return -EIO;
	}
	if (m_log.getMsgMask() & XrdHTTPServer::Debug) {
		std::stringstream ss;
		ss << "Stat on object returned " << objInfo.size() << " objects and "
		   << commonPrefixes.size() << " prefixes";
		m_log.Log(XrdHTTPServer::Debug, "Stat", ss.str().c_str());
	}

	// Recall we queried for 'object name' + '/'; as in, 'foo/'
	// instead of 'foo'.
	// If there's an object name with a trailing '/', then we
	// aren't able to open it or otherwise represent it within
	// XRootD.  Hence, we just pretend it doesn't exist.
	bool foundObj = false;
	for (const auto &obj : objInfo) {
		if (obj.m_key == object) {
			foundObj = true;
			break;
		}
	}
	if (foundObj) {
		return -ENOENT;
	}

	if (!objInfo.size() && !commonPrefixes.size()) {
		return -ENOENT;
	}

	if (buff) {
		memset(buff, '\0', sizeof(struct stat));
		buff->st_mode = 0700 | S_IFDIR;
		buff->st_nlink = 0;
		buff->st_uid = 1;
		buff->st_gid = 1;
		buff->st_size = 4096;
		buff->st_mtime = buff->st_atime = buff->st_ctime = 0;
		buff->st_dev = 0;
		buff->st_ino = 1;
	}
	return 0;
}

int S3FileSystem::Create(const char *tid, const char *path, mode_t mode,
						 XrdOucEnv &env, int opts) {
	// Is path valid?
	std::string exposedPath, object;
	int rv = parsePath(path, exposedPath, object);
	if (rv != 0) {
		return rv;
	}

	//
	// We could instead invoke the upload mchinery directly to create a
	// 0-byte file, but it seems smarter to remove a round-trip (in
	// S3File::Open(), checking if the file exists) than to add one
	// (here, creating the file if it doesn't exist).
	//

	return 0;
}

int S3FileSystem::parsePath(const char *fullPath, std::string &exposedPath,
							std::string &object) const {
	//
	// Check the path for validity.
	//
	std::filesystem::path p(fullPath);
	auto pathComponents = p.begin();

	// Iterate through components of the fullPath until we either find a match
	// or we've reached the end of the path.
	std::filesystem::path currentPath = *pathComponents;
	while (pathComponents != p.end()) {
		if (exposedPathExists(currentPath.string())) {
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
		object = "";
		return 0;
	}

	std::filesystem::path objectPath = *pathComponents++;
	for (; pathComponents != p.end(); ++pathComponents) {
		objectPath /= (*pathComponents);
	}
	object = objectPath.string();

	return 0;
}

const std::shared_ptr<S3AccessInfo>
S3FileSystem::getS3AccessInfo(const std::string &exposedPath,
							  std::string &object) const {
	auto ai = m_s3_access_map.at(exposedPath);
	if (!ai) {
		return ai;
	}
	if (ai->getS3BucketName().empty()) {
		// Bucket name is embedded in the "object" name.  Split it into the
		// bucket and "real" object.
		std::shared_ptr<S3AccessInfo> aiCopy(new S3AccessInfo(*ai));
		auto firstSlashIdx = object.find('/');
		if (firstSlashIdx == std::string::npos) {
			aiCopy->setS3BucketName(object);
			object = "";
		} else {
			aiCopy->setS3BucketName(object.substr(0, firstSlashIdx));
			object = object.substr(firstSlashIdx + 1);
		}
		return aiCopy;
	}
	return ai;
}
