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
	: XrdOssWrapper(*oss), m_oss(oss), m_log(lp, "globus_") {
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

	bool saw_blocks = false;
	bool in_block = false;
	GlobusRouteConfig default_route;
	std::shared_ptr<GlobusRouteConfig> route(new GlobusRouteConfig(default_route));

	m_log.setMsgMask(0);

	while (globus_conf.GetLine()) {
		std::string attribute = globus_conf.GetToken();
		if (!strcmp(attribute.c_str(), "globus.trace")) {
			if (!XrdHTTPServer::ConfigLog(globus_conf, m_log)) {
				m_log.Emsg("Config", "Failed to configure the log level");
			}
			continue;
		}

		if (!strcmp(attribute.c_str(), "globus.begin")) {
			if (in_block) {
				m_log.Emsg("Config", "Nested globus.begin blocks are not allowed");
				return false;
			}
			in_block = true;
			saw_blocks = true;
			route.reset(new GlobusRouteConfig(default_route));
			continue;
		}
		if (!strcmp(attribute.c_str(), "globus.end")) {
			if (!in_block) {
				m_log.Emsg("Config", "Encountered globus.end without matching begin");
				return false;
			}
			if (route->storage_prefix.empty() || route->endpoint_path.empty() ||
				route->transfer_url.empty()) {
				m_log.Emsg("Config", "globus route is missing one or more required settings");
				return false;
			}
			if (route->storage_prefix[0] != '/') {
				route->storage_prefix = "/" + route->storage_prefix;
			}
			m_routes[route->storage_prefix] = route;
			in_block = false;
			continue;
		}

		auto value = globus_conf.GetToken();
		if (!value) {
			continue;
		}

		auto &target = in_block ? *route : default_route;

		if (!handle_required_config(attribute, "globus.endpoint_path", value,
									target.endpoint_path) ||
			!handle_required_config(attribute, "globus.storage_prefix", value,
									target.storage_prefix) ||
			!handle_required_config(attribute, "globus.transfer_url_base",
									value, target.transfer_url)) {
			return false;
		}
		if (!strcmp(attribute.c_str(), "globus.transfer_token_file")) {
			target.transfer_token = std::make_shared<TokenFile>(value, &m_log);
		}
	}

	if (in_block) {
		m_log.Emsg("Config", "Encountered unterminated globus.begin block");
		return false;
	}

	if (!saw_blocks) {
		auto final_route = std::make_shared<GlobusRouteConfig>(default_route);
		if (final_route->storage_prefix.empty() || final_route->endpoint_path.empty() ||
			final_route->transfer_url.empty()) {
			m_log.Emsg("Config", "globus route is missing one or more required settings");
			return false;
		}
		if (final_route->storage_prefix[0] != '/') {
			final_route->storage_prefix = "/" + final_route->storage_prefix;
		}
		m_routes[final_route->storage_prefix] = final_route;
	}

	return true;
}

int GlobusFileSystem::ResolvePath(const std::string &path,
								  const GlobusRouteConfig *&route,
								  std::string &relative_path) const {
	const GlobusRouteConfig *best_route = nullptr;
	std::string best_object;
	size_t best_len = 0;
	for (const auto &entry : m_routes) {
		std::string candidate_object;
		if (parse_path(entry.second->storage_prefix, path.c_str(),
					   candidate_object) != 0) {
			continue;
		}
		size_t candidate_len = entry.second->storage_prefix.size();
		if (!best_route || candidate_len > best_len) {
			best_route = entry.second.get();
			best_object = candidate_object;
			best_len = candidate_len;
		}
	}
	if (!best_route) {
		return -ENOENT;
	}
	route = best_route;
	relative_path = best_object;
	if (relative_path.empty()) {
		relative_path = "/";
	} else if (relative_path[0] != '/') {
		relative_path = "/" + relative_path;
	}
	return 0;
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
	const GlobusRouteConfig *route = nullptr;
	std::string relative_path;
	int rv = ResolvePath(path, route, relative_path);
	if (rv != 0) {
		return rv;
	}

	m_log.Log(LogMask::Debug, "GlobusFileSystem::Stat", "Stat'ing path",
			  relative_path.c_str());

	auto token = route->transfer_token.get();
	if (!token) {
		m_log.Emsg("Stat", "Failed to get transfer token");
		return -ENOENT;
	}

	HTTPDownload statCommand(getStatUrl(*route, relative_path), "", m_log, token);
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

	const GlobusRouteConfig *route = nullptr;
	std::string relative_path;
	int rv = ResolvePath(path, route, relative_path);
	if (rv != 0) {
		return rv;
	}

	auto token = route->transfer_token.get();
	if (!token) {
		m_log.Emsg("Mkdir", "Failed to get transfer token");
		return -ENOENT;
	}
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
		GlobusMkdirRequest mkdirCommand(getMkdirUrl(*route), m_log, token);
		nlohmann::json body = {{"DATA_TYPE", "mkdir"},
							   {"path", buildEndpointPath(*route, relative_path)}};
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
		int stat_rv = Stat((route->storage_prefix + prefixes[idx]).c_str(), &sb, 0, env);
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
		GlobusMkdirRequest mkdirCommand(getMkdirUrl(*route), m_log, token);
		nlohmann::json body = {{"DATA_TYPE", "mkdir"},
							   {"path", buildEndpointPath(*route, prefixes[idx])}};
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
GlobusFileSystem::getLsUrl(const GlobusRouteConfig &route,
						   const std::string &relative_path) const {
	return getOperationUrl(route, "ls", relative_path);
}

const std::string
GlobusFileSystem::getStatUrl(const GlobusRouteConfig &route,
							 const std::string &relative_path) const {
	return getOperationUrl(route, "stat", relative_path);
}

const std::string GlobusFileSystem::getMkdirUrl(const GlobusRouteConfig &route) const {
	return getOperationUrl(route, "mkdir");
}

std::string
GlobusFileSystem::buildEndpointPath(const GlobusRouteConfig &route,
									const std::string &relative_path) const {
	std::string endpoint = route.endpoint_path.empty() ? "/" : route.endpoint_path;
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
GlobusFileSystem::getOperationUrl(const GlobusRouteConfig &route,
								  const std::string &operation,
								  const std::string &relative_path) const {
	if (route.transfer_url.empty()) {
		return "";
	}

	std::string transfer_url = route.transfer_url;
	if (!route.endpoint_path.empty()) {
		std::string ep = route.endpoint_path;
		if (!ep.empty() && ep.back() == '/') {
			ep.pop_back();
		}
		transfer_url += "/%s?path=" + ep;
	}
	size_t format_pos = transfer_url.find("%s");
	std::string result = transfer_url;
	result.replace(format_pos, 2, operation);

	// Append the relative path to the URL
	if (!relative_path.empty()) {
		result += relative_path;
	}

	return result;
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
