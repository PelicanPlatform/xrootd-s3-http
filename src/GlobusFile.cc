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

#include <memory>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

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
