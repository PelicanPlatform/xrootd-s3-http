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

#include "HTTPFileSystem.hh"
#include "HTTPDirectory.hh"
#include "HTTPFile.hh"
#include "logging.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucGatherConf.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdVersion.hh>

#include <memory>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "stl_string_utils.hh"

using namespace XrdHTTPServer;

namespace {

std::string normalize_http_prefix(const std::string &prefix) {
	if (prefix.empty() || prefix[0] == '/') {
		return prefix;
	}
	return "/" + prefix;
}

bool validate_http_route(XrdSysError &log,
						 const HTTPFileSystem::HTTPRouteConfig &route) {
	if (route.url_base.empty()) {
		if (route.host_name.empty()) {
			log.Emsg("Config", "httpserver.host_name not specified; this or "
							 "httpserver.url_base are required");
			return false;
		}
		if (route.host_url.empty()) {
			log.Emsg("Config", "httpserver.host_url not specified; this or "
							 "httpserver.url_base are required");
			return false;
		}
	}
	if (route.remote_flavor != "http" && route.remote_flavor != "webdav" &&
		route.remote_flavor != "auto") {
		log.Emsg("Config", "Invalid httpserver.remote_flavor specified; "
						 "must be one of: 'http', 'webdav', or 'auto'");
		return false;
	}
	return true;
}

} // namespace

HTTPFileSystem::HTTPFileSystem(XrdSysLogger *lp, const char *configfn,
							   XrdOucEnv * /*envP*/)
	: m_log(lp, "httpserver_") {
	m_log.Say("------ Initializing the HTTP filesystem plugin.");
	if (!Config(lp, configfn)) {
		throw std::runtime_error("Failed to configure HTTP filesystem plugin.");
	}
}

HTTPFileSystem::~HTTPFileSystem() {}

bool HTTPFileSystem::handle_required_config(const std::string &name_from_config,
											const char *desired_name,
											const std::string &source,
											std::string &target) {
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

bool HTTPFileSystem::Config(XrdSysLogger *lp, const char *configfn) {
	XrdOucEnv myEnv;
	XrdOucGatherConf httpserver_conf("httpserver.", &m_log);
	int result;
	if ((result = httpserver_conf.Gather(configfn,
										 XrdOucGatherConf::full_lines)) < 0) {
		m_log.Emsg("Config", -result, "parsing config file", configfn);
		return false;
	}

	bool saw_blocks = false;
	bool in_block = false;
	HTTPRouteConfig default_route;
	default_route.remote_flavor = "auto";
	std::shared_ptr<HTTPRouteConfig> route(new HTTPRouteConfig(default_route));

	m_log.setMsgMask(0);

	while (httpserver_conf.GetLine()) {
		auto attribute = httpserver_conf.GetToken();
		if (!strcmp(attribute, "httpserver.trace")) {
			if (!XrdHTTPServer::ConfigLog(httpserver_conf, m_log)) {
				m_log.Emsg("Config", "Failed to configure the log level");
			}
			continue;
		}

		if (!strcmp(attribute, "httpserver.begin")) {
			if (in_block) {
				m_log.Emsg("Config", "Nested httpserver.begin blocks are not allowed");
				return false;
			}
			in_block = true;
			saw_blocks = true;
			route.reset(new HTTPRouteConfig(default_route));
			continue;
		}
		if (!strcmp(attribute, "httpserver.end")) {
			if (!in_block) {
				m_log.Emsg("Config", "Encountered httpserver.end without matching begin");
				return false;
			}
			route->storage_prefix = normalize_http_prefix(route->storage_prefix);
			if (!validate_http_route(m_log, *route)) {
				return false;
			}
			auto prefix = route->matchPrefix();
			if (prefix.empty()) {
				m_log.Emsg("Config", "httpserver route prefix must not be empty");
				return false;
			}
			m_routes[prefix] = route;
			in_block = false;
			continue;
		}

		auto value = httpserver_conf.GetToken();
		if (!value) {
			continue;
		}

		auto &target = in_block ? *route : default_route;

		if (!handle_required_config(attribute, "httpserver.host_name", value,
									target.host_name) ||
			!handle_required_config(attribute, "httpserver.host_url", value,
									target.host_url) ||
			!handle_required_config(attribute, "httpserver.url_base", value,
									target.url_base) ||
			!handle_required_config(attribute, "httpserver.remote_flavor",
									value, target.remote_flavor) ||
			!handle_required_config(attribute, "httpserver.storage_prefix",
									value, target.storage_prefix)) {
			return false;
		}
		if (!strcmp(attribute, "httpserver.token_file")) {
			target.token = std::make_shared<TokenFile>(value, &m_log);
		}
	}

	if (in_block) {
		m_log.Emsg("Config", "Encountered unterminated httpserver.begin block");
		return false;
	}

	if (!saw_blocks) {
		auto final_route = std::make_shared<HTTPRouteConfig>(default_route);
		final_route->storage_prefix = normalize_http_prefix(final_route->storage_prefix);
		if (!validate_http_route(m_log, *final_route)) {
			return false;
		}
		auto prefix = final_route->matchPrefix();
		if (prefix.empty()) {
			m_log.Emsg("Config", "httpserver route prefix must not be empty");
			return false;
		}
		m_routes[prefix] = final_route;
	}

	return true;
}

int HTTPFileSystem::ResolvePath(const char *path,
								const HTTPRouteConfig *&route,
								std::string &object) const {
	const HTTPRouteConfig *best_route = nullptr;
	std::string best_object;
	size_t best_len = 0;
	for (const auto &entry : m_routes) {
		std::string candidate_object;
		if (parse_path(entry.second->matchPrefix(), path, candidate_object) != 0) {
			continue;
		}
		size_t candidate_len = entry.second->matchPrefix().size();
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
	object = best_object;
	return 0;
}

// Object Allocation Functions
//
XrdOssDF *HTTPFileSystem::newDir(const char *user) {
	return new HTTPDirectory(m_log, *this);
}

XrdOssDF *HTTPFileSystem::newFile(const char *user) {
	return new HTTPFile(m_log, this);
}

int HTTPFileSystem::Stat(const char *path, struct stat *buff, int opts,
						 XrdOucEnv *env) {
	std::string error;

	// need to forward a HEAD request to the remote server

	HTTPFile httpFile(m_log, this);
	int rv = httpFile.Open(path, 0, (mode_t)0, *env);
	if (rv && rv != -EISDIR) {
		m_log.Emsg("Stat", "Failed to open path:", path);
		return rv;
	}
	// Assume that HTTPFile::FStat() doesn't write to buff unless it succeeds.
	return httpFile.Fstat(buff);
}

int HTTPFileSystem::Create(const char *tid, const char *path, mode_t mode,
						   XrdOucEnv &env, int opts) {
	// Is path valid?
	std::string object;
	const HTTPRouteConfig *route = nullptr;
	int rv = ResolvePath(path, route, object);
	if (rv != 0) {
		return rv;
	}

	return 0;
}

int HTTPFileSystem::Unlink(const char *path, int Opts, XrdOucEnv *env) {
	m_log.Log(LogMask::Debug, "Unlink", "Unlinking path", path);
	// make sure file exists
	HTTPFile httpFile(m_log, this);
	if (httpFile.Open(path, 0, (mode_t)0, *env) != 0) {
		m_log.Emsg("Unlink", "Failed to open path:", path);
		return -ENOENT;
	}

	std::string object;
	const HTTPRouteConfig *route = nullptr;
	if (ResolvePath(path, route, object) != 0) {
		m_log.Emsg("Unlink", "Failed to parse path:", path);
		return -EIO;
	}
	// delete the file
	std::string hostUrl =
		!route->url_base.empty() ? route->url_base : route->host_url;
	m_log.Log(LogMask::Debug, "Unlink", "Object:", object.c_str());
	m_log.Log(LogMask::Debug, "Unlink", "Host URL:", hostUrl.c_str());
	HTTPDelete deleteCommand(hostUrl, object, m_log, route->token.get());
	if (!deleteCommand.SendRequest()) {
		return HTTPRequest::HandleHTTPError(deleteCommand, m_log, "DELETE",
											object.c_str());
	}

	return 0;
}
