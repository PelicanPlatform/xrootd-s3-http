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
#include <XrdOuc/XrdOucStream.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdVersion.hh>

#include <filesystem>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

S3FileSystem::S3FileSystem(XrdSysLogger *lp, const char *configfn,
						   XrdOucEnv *envP)
	: m_env(envP), m_log(lp, "s3_") {
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
	XrdOucStream Config(&m_log, getenv("XRDINSTANCE"), &myEnv, "=====> ");

	int cfgFD = open(configfn, O_RDONLY, 0);
	if (cfgFD < 0) {
		m_log.Emsg("Config", errno, "open config file", configfn);
		return false;
	}

	char *temporary;
	std::string value;
	std::string attribute;
	Config.Attach(cfgFD);
	std::shared_ptr<S3AccessInfo> newAccessInfo(new S3AccessInfo());
	std::string exposedPath;
	m_log.setMsgMask(0);
	while ((temporary = Config.GetMyFirstWord())) {
		attribute = temporary;
		if (attribute == "s3.trace") {
			if (!XrdHTTPServer::ConfigLog(Config, m_log)) {
				m_log.Emsg("Config", "Failed to configure the log level");
			}
			continue;
		}

		temporary = Config.GetWord();
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
			Config.Close();
			return false;
		}
		if (!handle_required_config("s3.bucket_name", value)) {
			Config.Close();
			return false;
		}
		if (!handle_required_config("s3.service_name", value)) {
			Config.Close();
			return false;
		}
		if (!handle_required_config("s3.region", value)) {
			Config.Close();
			return false;
		}
		if (!handle_required_config("s3.service_url", value)) {
			Config.Close();
			return false;
		}
		if (!handle_required_config("s3.access_key_file", value)) {
			Config.Close();
			return false;
		}
		if (!handle_required_config("s3.secret_key_file", value)) {
			Config.Close();
			return false;
		}
		if (!handle_required_config("s3.url_style", value)) {
			Config.Close();
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

	int retc = Config.LastError();
	if (retc) {
		m_log.Emsg("Config", -retc, "read config file", configfn);
		Config.Close();
		return false;
	}

	Config.Close();
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

int S3FileSystem::Stat(const char *path, struct stat *buff, int opts,
					   XrdOucEnv *env) {
	m_log.Log(XrdHTTPServer::Debug, "Stat", "Stat'ing path", path);

	std::string exposedPath, object;
	auto rv = parsePath(path, exposedPath, object);
	if (rv != 0) {
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
	AmazonS3List listCommand(*ai, object, 1, m_log);
	auto res = listCommand.SendRequest("");
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
			case 500:
				return -EIO;
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

	if (object.empty()) {
		buff->st_mode = 0700 | S_IFDIR;
		buff->st_nlink = 0;
		buff->st_uid = 1;
		buff->st_gid = 1;
		buff->st_size = 4096;
		buff->st_mtime = buff->st_atime = buff->st_ctime = 0;
		buff->st_dev = 0;
		buff->st_ino = 1;
		return 0;
	}

	bool foundObj = false;
	size_t objSize = 0;
	for (const auto &obj : objInfo) {
		if (obj.m_key == object) {
			foundObj = true;
			objSize = obj.m_size;
			break;
		}
	}
	if (foundObj) {
		buff->st_mode = 0600 | S_IFREG;
		buff->st_nlink = 1;
		buff->st_uid = buff->st_gid = 1;
		buff->st_size = objSize;
		buff->st_mtime = buff->st_atime = buff->st_ctime = 0;
		buff->st_dev = 0;
		buff->st_ino = 1;
		return 0;
	}

	auto desiredPrefix = object + "/";
	bool foundPrefix = false;
	for (const auto &prefix : commonPrefixes) {
		if (prefix == desiredPrefix) {
			foundPrefix = true;
			break;
		}
	}
	if (!foundPrefix) {
		return -ENOENT;
	}

	buff->st_mode = 0700 | S_IFDIR;
	buff->st_nlink = 0;
	buff->st_uid = 1;
	buff->st_gid = 1;
	buff->st_size = 4096;
	buff->st_mtime = buff->st_atime = buff->st_ctime = 0;
	buff->st_dev = 0;
	buff->st_ino = 1;
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
		return -ENOENT;
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
