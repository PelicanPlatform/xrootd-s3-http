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

GlobusFileSystem::GlobusFileSystem(XrdSysLogger *lp, const char *configfn,
								   XrdOucEnv *envP)
	: HTTPFileSystem(lp, configfn, envP), m_log(lp, "globus_"), 
	  m_globus_token("", &m_log) {
	m_log.Say("------ Initializing the Globus filesystem plugin.");
	if (!Config(lp, configfn)) {
		throw std::runtime_error("Failed to configure Globus filesystem plugin.");
	}
}

GlobusFileSystem::~GlobusFileSystem() {}

bool GlobusFileSystem::handle_required_config(const std::string &name_from_config,
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

bool GlobusFileSystem::Config(XrdSysLogger *lp, const char *configfn) {
	// First, call the parent HTTPFileSystem config
	if (!HTTPFileSystem::Config(lp, configfn)) {
		return false;
	}

	XrdOucEnv myEnv;
	XrdOucGatherConf globus_conf("globus.", &m_log);
	int result;
	if ((result = globus_conf.Gather(configfn,
									 XrdOucGatherConf::full_lines)) < 0) {
		m_log.Emsg("Config", -result, "parsing config file", configfn);
		return false;
	}

	std::string attribute;
	std::string globus_token_file;

	m_log.setMsgMask(0);

	while (globus_conf.GetLine()) {
		auto attribute = globus_conf.GetToken();
		if (!strcmp(attribute, "globus.trace")) {
			if (!XrdHTTPServer::ConfigLog(globus_conf, m_log)) {
				m_log.Emsg("Config", "Failed to configure the log level");
			}
			continue;
		}

		auto value = globus_conf.GetToken();
		if (!value) {
			continue;
		}

		if (!handle_required_config(attribute, "globus.endpoint", value,
									m_globus_endpoint) ||
			!handle_required_config(attribute, "globus.collection_id", value,
									m_globus_collection_id) ||
			!handle_required_config(attribute, "globus.token_file", value,
									globus_token_file)) {
			return false;
		}
	}

	// Validate required Globus-specific configuration
	if (m_globus_endpoint.empty()) {
		m_log.Emsg("Config", "globus.endpoint not specified");
		return false;
	}

	if (m_globus_collection_id.empty()) {
		m_log.Emsg("Config", "globus.collection_id not specified");
		return false;
	}

	// Configure Globus token if specified
	if (!globus_token_file.empty()) {
		m_globus_token = TokenFile(globus_token_file, &m_log);
	}

	return true;
}

// Object Allocation Functions
//
XrdOssDF *GlobusFileSystem::newDir(const char *user) {
	return new GlobusDirectory(m_log, this);
}

XrdOssDF *GlobusFileSystem::newFile(const char *user) {
	return new HTTPFile(m_log, this);
} 