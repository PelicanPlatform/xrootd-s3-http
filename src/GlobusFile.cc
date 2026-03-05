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

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

int GlobusFile::Open(const char *path, int Oflag, mode_t Mode, XrdOucEnv &env) {
	// For any write-intent open, ensure the parent directory exists.
	// XRootD's HTTP handler sends O_RDWR|O_TRUNC (not O_CREAT) for PUT
	// requests, so we gate on write access mode rather than O_CREAT.
	// We check the *parent* rather than the file itself so that a
	// missing-file ENOENT never incorrectly triggers mkdir; we only create
	// directories when they are actually absent.
	if ((Oflag & O_ACCMODE) != O_RDONLY) {
		auto parent_path = std::filesystem::path(path).parent_path();
		if (!parent_path.empty() && parent_path != "/") {
			struct stat sb;
			int stat_rv = m_oss->Stat(parent_path.c_str(), &sb, 0, &env);
			if (stat_rv == -ENOENT) {
				m_log.Log(XrdHTTPServer::LogMask::Info, "GlobusFile::Open",
						  "Parent directory missing, creating",
						  parent_path.c_str());
				int mkdir_rv = m_oss->Mkdir(parent_path.c_str(), 0755,
											/*mkpath=*/1, &env);
				if (mkdir_rv != 0 && mkdir_rv != -EEXIST) {
					m_log.Log(XrdHTTPServer::LogMask::Error, "GlobusFile::Open",
							  "Failed to create parent path",
							  parent_path.c_str(), strerror(-mkdir_rv));
					return mkdir_rv;
				}
				m_log.Log(XrdHTTPServer::LogMask::Info, "GlobusFile::Open",
						  "Successfully created parent path",
						  parent_path.c_str());
			} else if (stat_rv != 0) {
				// Unexpected stat error — propagate it.
				m_log.Log(XrdHTTPServer::LogMask::Error, "GlobusFile::Open",
						  "Stat of parent path failed",
						  parent_path.c_str(), strerror(-stat_rv));
				return stat_rv;
			}
		}
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
