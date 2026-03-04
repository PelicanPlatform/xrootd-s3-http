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

#include "GlobusFile.hh"
#include "GlobusFileSystem.hh"
#include "HTTPCommands.hh"
#include "logging.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucStream.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdVersion.hh>

// #include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>

#include <XProtocol/XProtocol.hh>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

int GlobusFile::Open(const char *path, int Oflag, mode_t Mode, XrdOucEnv &env) {
	
	if (Oflag & kXR_mkpath) {
		struct stat sb;
		auto stat_rv = m_oss->Stat(path, &sb, 0, &env);
		if (stat_rv == -ENOENT) {
			bool should_create_parent = false;
			{
				std::lock_guard<std::mutex> guard(m_mkpath_attempt_mutex);
				if (!m_pending_mkpath_attempt) {
					m_pending_mkpath_attempt = true;
				} else {
					should_create_parent = true;
					m_pending_mkpath_attempt = false;
				}
			}

			if (!should_create_parent) {
				m_log.Log(XrdHTTPServer::LogMask::Debug, "GlobusFile::Open",
						  "mkpath requested; deferring mkdir until second open "
						  "attempt",
						  path);
				return -ENOENT;
			}

			auto parent_path = std::filesystem::path(path).parent_path();
			if (!parent_path.empty()) {
				auto mkdir_rv = m_oss->Mkdir(parent_path.c_str(), 0755,
											 /*mkpath=*/1, &env);
				if (mkdir_rv != 0 && mkdir_rv != -EEXIST) {
					m_log.Log(XrdHTTPServer::LogMask::Error, "GlobusFile::Open",
							  "Failed to create parent path on second mkpath "
							  "attempt",
							  parent_path.c_str(), strerror(-mkdir_rv));
					return mkdir_rv;
				}
			}
		} else {
			std::lock_guard<std::mutex> guard(m_mkpath_attempt_mutex);
			m_pending_mkpath_attempt = false;
		}
	} else {
		std::lock_guard<std::mutex> guard(m_mkpath_attempt_mutex);
		m_pending_mkpath_attempt = false;
	}

	return m_wrapped->Open(path, Oflag, Mode, env);
}

extern "C" {

/*
	The GlobusFileSystem adds Globus-specific functionality on top of the
	HTTPFileSystem.  Rather than re-implement or re-compile things, for now,
	we simply assume we wrap on top.
*/
XrdOss *XrdOssAddStorageSystem2(XrdOss *curr_oss, XrdSysLogger *logger,
								const char *config_fn, const char *parms,
								XrdOucEnv *envP) {
	XrdSysError log(logger, "globus_");
	try {
		HTTPRequest::Init(log);
		GlobusFileSystem *new_oss =
			new GlobusFileSystem(curr_oss, logger, config_fn, envP);
		return new_oss;
	} catch (std::runtime_error &re) {
		log.Emsg("Initialize", "Encountered a runtime failure", re.what());
		return nullptr;
	}
}

} // end extern "C"

XrdVERSIONINFO(XrdOssAddStorageSystem2, globus);
