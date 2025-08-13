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

HTTPFileSystem::HTTPFileSystem(XrdSysLogger *lp, const char *configfn,
							   XrdOucEnv * /*envP*/)
	: m_log(lp, "httpserver_"), m_token("", &m_log) {
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

	std::string attribute;
	std::string token_file;

	m_log.setMsgMask(0);

	while (httpserver_conf.GetLine()) {
		auto attribute = httpserver_conf.GetToken();
		if (!strcmp(attribute, "httpserver.trace")) {
			if (!XrdHTTPServer::ConfigLog(httpserver_conf, m_log)) {
				m_log.Emsg("Config", "Failed to configure the log level");
			}
			continue;
		}

		auto value = httpserver_conf.GetToken();
		if (!value) {
			continue;
		}

		if (!handle_required_config(attribute, "httpserver.host_name", value,
									http_host_name) ||
			!handle_required_config(attribute, "httpserver.host_url", value,
									http_host_url) ||
			!handle_required_config(attribute, "httpserver.url_base", value,
									m_url_base) ||
			!handle_required_config(attribute, "httpserver.remote_flavor",
									value, m_remote_flavor) ||
			!handle_required_config(attribute, "httpserver.storage_prefix",
									value, m_storage_prefix) ||
			!handle_required_config(attribute, "httpserver.token_file", value,
									token_file)) {
			return false;
		}
	}

	if (m_url_base.empty()) {
		if (http_host_name.empty()) {
			m_log.Emsg("Config", "httpserver.host_name not specified; this or "
								 "httpserver.url_base are required");
			return false;
		}
		if (http_host_url.empty()) {
			m_log.Emsg("Config", "httpserver.host_url not specified; this or "
								 "httpserver.url_base are required");
			return false;
		}
		if (m_remote_flavor != "http" && m_remote_flavor != "webdav" &&
			m_remote_flavor != "auto") {
			m_log.Emsg("Config", "Invalid httpserver.remote_flavor specified; "
								 "must be one of: 'http', 'webdav', or 'auto'");
			return false;
		}
	}

	if (!token_file.empty()) {
		m_token = TokenFile(token_file, &m_log);
	}

	return true;
}

// Object Allocation Functions
//
XrdOssDF *HTTPFileSystem::newDir(const char *user) {
	return new HTTPDirectory(m_log, this);
}

XrdOssDF *HTTPFileSystem::newFile(const char *user) {
	return new HTTPFile(m_log, this);
}

int HTTPFileSystem::Stat(const char *path, struct stat *buff, int opts,
						 XrdOucEnv *env) {
	std::string error;

	m_log.Emsg("Stat", "Stat'ing path", path);

	// need to forward a HEAD request to the remote server

	HTTPFile httpFile(m_log, this);
	int rv = httpFile.Open(path, 0, (mode_t)0, *env);
	if (rv && rv != EISDIR) {
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
	std::string hostname = this->getHTTPHostName();
	int rv = parse_path(hostname, path, object);
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
	if (parse_path(getStoragePrefix(), path, object) != 0) {
		m_log.Emsg("Unlink", "Failed to parse path:", path);
		return -EIO;
	}
	// delete the file
	std::string hostUrl =
		!getHTTPUrlBase().empty() ? getHTTPUrlBase() : getHTTPHostUrl();
	m_log.Log(LogMask::Debug, "Unlink", "Object:", object.c_str());
	m_log.Log(LogMask::Debug, "Unlink", "Host URL:", hostUrl.c_str());
	HTTPDelete deleteCommand(hostUrl, object, m_log, &m_token);
	if (!deleteCommand.SendRequest()) {
		return HTTPRequest::HandleHTTPError(deleteCommand, m_log, "DELETE",
											object.c_str());
	}

	return 0;
}
