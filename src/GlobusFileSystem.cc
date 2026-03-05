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

#include "GlobusFileSystem.hh"
#include "GlobusDirectory.hh"
#include "GlobusFile.hh"
#include "logging.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucGatherConf.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdVersion.hh>

#include <memory>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "HTTPCommands.hh"
#include "stl_string_utils.hh"
#include <nlohmann/json.hpp>

using namespace XrdHTTPServer;

namespace {

// Globus Transfer API mkdir request.
class GlobusMkdirRequest final : public HTTPRequest {
  public:
	GlobusMkdirRequest(const std::string &url, XrdSysError &log,
					   const TokenFile *token)
		: HTTPRequest(url, log, token) {}

	bool SendRequest(const std::string &payload) {
		httpVerb = "POST";
		expectedResponseCode = {202};
		headers["Content-Type"] = "application/json";
		return SendHTTPRequest(payload);
	}
};

} // namespace

GlobusFileSystem::GlobusFileSystem(XrdOss *oss, XrdSysLogger *lp,
								   const char *configfn, XrdOucEnv *envP)
	: XrdOssWrapper(*oss), m_oss(oss), m_log(lp, "globus_"),
	  m_transfer_token("", nullptr) {
	m_log.Say("------ Initializing the Globus filesystem plugin.");

	if (!Config(lp, configfn)) {
		throw std::runtime_error(
			"Failed to configure Globus filesystem plugin.");
	}
}

GlobusFileSystem::~GlobusFileSystem() {}

bool GlobusFileSystem::handle_required_config(
	const std::string &name_from_config, const char *desired_name,
	const std::string &source, std::string &target) {
	if (name_from_config != desired_name) {
		return true;
	}

	if (source.empty()) {
		std::string error;
		formatstr(error, "%s must specify a value", desired_name);
		m_log.Emsg("Config", error.c_str());
		return false;
	}

	std::stringstream ss;
	ss << "Setting " << desired_name << "=" << source;
	m_log.Log(LogMask::Debug, "Config", ss.str().c_str());
	target = source;
	return true;
}

bool GlobusFileSystem::Config(XrdSysLogger *lp, const char *configfn) {
	// note that this in theory should wrap the HTTPFileSystem, so we don't
	// need to call the parent HTTPFileSystem config

	XrdOucEnv myEnv;
	XrdOucGatherConf globus_conf("globus.", &m_log);
	int result;
	if ((result = globus_conf.Gather(configfn, XrdOucGatherConf::full_lines)) <
		0) {
		m_log.Emsg("Config", -result, "parsing config file", configfn);
		return false;
	}

	std::string attribute;
	std::string transfer_token_file;
	std::string endpoint_path;

	m_log.setMsgMask(0);

	while (globus_conf.GetLine()) {
		attribute = globus_conf.GetToken();
		if (!strcmp(attribute.c_str(), "globus.trace")) {
			if (!XrdHTTPServer::ConfigLog(globus_conf, m_log)) {
				m_log.Emsg("Config", "Failed to configure the log level");
			}
			continue;
		}

		auto value = globus_conf.GetToken();
		if (!value) {
			continue;
		}

		if (!handle_required_config(attribute, "globus.endpoint_path", value,
									endpoint_path) ||
			!handle_required_config(attribute, "globus.storage_prefix", value,
									m_storage_prefix) ||
			!handle_required_config(attribute, "globus.transfer_url_base",
									value, m_transfer_url) ||
			!handle_required_config(attribute, "globus.transfer_token_file",
									value, transfer_token_file)) {
			return false;
		}
	}

	// Build the complete URLs
	m_endpoint_path = endpoint_path;
	if (!m_transfer_url.empty() && !endpoint_path.empty()) {
		// Strip the trailing slash from endpoint_path before embedding it in
		// the URL template.  extractRelativePath() always returns a path that
		// starts with '/', so concatenating a trailing slash here would produce
		// a double-slash (e.g. "?path=//top_level_path/custom_path").  Globus interprets
		// "?path=//" as the root path and returns a 200 instead of a 404,
		// causing parent-directory existence checks to spuriously succeed.
		std::string ep = endpoint_path;
		if (!ep.empty() && ep.back() == '/') {
			ep.pop_back();
		}
		m_transfer_url += "/%s?path=" + ep;
	}

	if (!transfer_token_file.empty()) {
		m_transfer_token = TokenFile(transfer_token_file, &m_log);
	}

	return true;
}

XrdOssDF *GlobusFileSystem::newDir(const char *user) {
	return new GlobusDirectory(m_log, *this);
}

XrdOssDF *GlobusFileSystem::newFile(const char *user) {
	std::unique_ptr<XrdOssDF> wrapped(wrapPI.newFile(user));
	return new GlobusFile(std::move(wrapped), m_log, this);
}

int GlobusFileSystem::Stat(const char *path, struct stat *buff, int opts,
						   XrdOucEnv *env) {
	// Extract the part of path that comes after the storage prefix
	std::string relative_path = extractRelativePath(path);

	m_log.Log(LogMask::Debug, "GlobusFileSystem::Stat", "Stat'ing path",
			  relative_path.c_str());

	auto token = getTransferToken();
	if (!token) {
		m_log.Emsg("Stat", "Failed to get transfer token");
		return -ENOENT;
	}

	HTTPDownload statCommand(getStatUrl(relative_path), "", m_log, token);
	if (!statCommand.SendRequest(0, 0)) {
		return HTTPRequest::HandleHTTPError(statCommand, m_log, "GET", "");
	}

	std::string response = statCommand.getResultString();

	// Parse the JSON response and populate the stat buffer
	try {
		auto json_response = nlohmann::json::parse(response);

		// Initialize the stat buffer
		memset(buff, 0, sizeof(struct stat));

		// Set file type and permissions
		if (json_response.contains("type")) {
			std::string type = json_response["type"].get<std::string>();
			if (type == "dir") {
				buff->st_mode = S_IFDIR | 0755;
			} else if (type == "file") {
				buff->st_mode = S_IFREG | 0644;
			}
		}

		// Set file size
		if (json_response.contains("size")) {
			buff->st_size = json_response["size"].get<off_t>();
		}

		buff->st_uid = buff->st_gid = 1;

		if (json_response.contains("last_modified")) {
			std::string last_modified =
				json_response["last_modified"].get<std::string>();
			time_t timestamp = parseTimestamp(last_modified);
			if (timestamp != 0) {
				buff->st_mtime = timestamp;
				buff->st_atime = timestamp;
				buff->st_ctime = timestamp;
			}
		}

		// Set number of links (1 for regular files, 2 for directories)
		buff->st_nlink = (buff->st_mode & S_IFDIR) ? 2 : 1;
		return 0;

	} catch (const nlohmann::json::exception &e) {
		m_log.Log(LogMask::Error, "GlobusFileSystem::Stat",
				  "Failed to parse JSON response:", e.what());
		return -EIO;
	}
}

int GlobusFileSystem::Unlink(const char *path, int Opts, XrdOucEnv *env) {
	return m_oss->Unlink(path, Opts, env);
}

int GlobusFileSystem::Mkdir(const char *path, mode_t mode, int mkpath,
							XrdOucEnv *env) {
	(void)mode;

	if (!path) {
		return -ENOENT;
	}

	auto token = getTransferToken();
	if (!token) {
		m_log.Emsg("Mkdir", "Failed to get transfer token");
		return -ENOENT;
	}

	std::string relative_path = extractRelativePath(path);
	if (relative_path.empty()) {
		relative_path = "/";
	}
	while (relative_path.size() > 1 && relative_path.back() == '/') {
		relative_path.pop_back();
	}

	if (relative_path == "/") {
		return -EEXIST;
	}
	// Globus requires each path to be made with a single command; you cannot
	// submit an HTTP POST with the entire path if there are missing folders
	// anywhere in the path. In this case, it returns HTTP code 404

	if (!mkpath) {
		GlobusMkdirRequest mkdirCommand(getMkdirUrl(), m_log, token);
		nlohmann::json body = {{"DATA_TYPE", "mkdir"},
							   {"path", buildEndpointPath(relative_path)}};
		if (!mkdirCommand.SendRequest(body.dump())) {
			unsigned long httpCode = mkdirCommand.getResponseCode();
			// Globus mkdir semantics: 202 = success, 502 = exists, 403 = perms,
			// 404 = not found
			if (httpCode == 502) {
				return -EEXIST;
			}
			if (httpCode == 403) {
				return -EPERM;
			}
			if (httpCode == 404) {
				return -ENOENT;
			}

			return HTTPRequest::HandleHTTPError(mkdirCommand, m_log, "POST",
												relative_path.c_str());
		}
		return 0;
	}

	// Build all prefixes: /a, /a/b, /a/b/c.
	std::vector<std::string> prefixes;
	std::string current;
	std::stringstream ss(relative_path);
	std::string component;
	while (std::getline(ss, component, '/')) {
		if (component.empty()) {
			continue;
		}
		current += "/" + component;
		prefixes.push_back(current);
	}
	if (prefixes.empty()) {
		return 0;
	}

	// Probe from deepest directory back toward root to find the deepest
	// existing prefix. This avoids sweeping mkdir from the root every time.
	// The prefixes are relative paths (e.g. "/top_level_path/custom_path"), so we must
	// prepend the storage prefix before calling Stat so that
	// extractRelativePath can correctly strip the prefix and produce the right
	// Transfer API URL.  Without the storage prefix, extractRelativePath falls
	// back to "/" (root), making every Stat appear to succeed and causing the
	// actual mkdir loop to be skipped.
	int first_missing_idx = 0;
	for (int idx = static_cast<int>(prefixes.size()) - 1; idx >= 0; --idx) {
		struct stat sb;
		int stat_rv = Stat((m_storage_prefix + prefixes[idx]).c_str(), &sb, 0, env);
		if (stat_rv == 0) {
			if (!S_ISDIR(sb.st_mode)) {
				return -ENOTDIR;
			}
			first_missing_idx = idx + 1;
			break;
		}
		if (stat_rv != -ENOENT) {
			return stat_rv;
		}
	}

	// Create only missing suffix prefixes. Handle concurrency by accepting
	// EEXIST if another writer creates a directory between our Stat and Mkdir.
	for (int idx = first_missing_idx;
		 idx < static_cast<int>(prefixes.size()); ++idx) {
		GlobusMkdirRequest mkdirCommand(getMkdirUrl(), m_log, token);
		nlohmann::json body = {{"DATA_TYPE", "mkdir"},
							   {"path", buildEndpointPath(prefixes[idx])}};
		int rv = 0;
		if (!mkdirCommand.SendRequest(body.dump())) {
			unsigned long httpCode = mkdirCommand.getResponseCode();
			// Globus mkdir semantics: 202 = success, 502 = exists, 403 = perms.
			if (httpCode == 502) {
				rv = -EEXIST;
			} else if (httpCode == 403) {
				rv = -EPERM;
			} else if (httpCode == 404) {
				rv = -ENOENT;
			}

			if (rv == 0) {
				rv = HTTPRequest::HandleHTTPError(mkdirCommand, m_log, "POST",
												  prefixes[idx].c_str());
			}
		}
		if (rv != 0 && rv != -EEXIST) {
			return rv;
		}
	}
	return 0;
}

const std::string
GlobusFileSystem::getLsUrl(const std::string &relative_path) const {
	return getOperationUrl("ls", relative_path);
}

const std::string
GlobusFileSystem::getStatUrl(const std::string &relative_path) const {
	return getOperationUrl("stat", relative_path);
}

const std::string GlobusFileSystem::getMkdirUrl() const {
	return getOperationUrl("mkdir");
}

std::string
GlobusFileSystem::buildEndpointPath(const std::string &relative_path) const {
	std::string endpoint = m_endpoint_path.empty() ? "/" : m_endpoint_path;
	if (endpoint[0] != '/') {
		endpoint = "/" + endpoint;
	}
	while (endpoint.size() > 1 && endpoint.back() == '/') {
		endpoint.pop_back();
	}

	std::string rel = relative_path.empty() ? "/" : relative_path;
	if (rel[0] != '/') {
		rel = "/" + rel;
	}
	while (rel.size() > 1 && rel.back() == '/') {
		rel.pop_back();
	}

	if (rel == "/") {
		return endpoint;
	}
	if (endpoint == "/") {
		return rel;
	}
	return endpoint + rel;
}

const std::string
GlobusFileSystem::getOperationUrl(const std::string &operation,
								  const std::string &relative_path) const {
	if (m_transfer_url.empty()) {
		return "";
	}

	size_t format_pos = m_transfer_url.find("%s");
	std::string result = m_transfer_url;
	result.replace(format_pos, 2, operation);

	// Append the relative path to the URL
	if (!relative_path.empty()) {
		result += relative_path;
	}

	return result;
}

std::string
GlobusFileSystem::extractRelativePath(const std::string &path) const {
	std::string relative_path = "/";

	if (!m_storage_prefix.empty() && path.find(m_storage_prefix) == 0) {
		relative_path = path.substr(m_storage_prefix.length());
		if (relative_path.empty()) {
			relative_path = "/";
		} else if (relative_path[0] != '/') {
			relative_path = "/" + relative_path;
		}
	}

	return relative_path;
}

time_t GlobusFileSystem::parseTimestamp(const std::string &last_modified) {
	if (!last_modified.empty()) {
		struct tm tm_time = {};
		if (strptime(last_modified.c_str(), "%Y-%m-%d %H:%M:%S", &tm_time) !=
			nullptr) {
			return mktime(&tm_time);
		}
	}
	return 0;
}
