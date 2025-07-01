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

#pragma once

#include "HTTPFileSystem.hh"
#include "TokenFile.hh"

#include <XrdOss/XrdOss.hh>
#include <XrdOuc/XrdOucStream.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSys/XrdSysPthread.hh>
#include <XrdVersion.hh>

#include <memory>
#include <string>

class GlobusFileSystem : public HTTPFileSystem {
  public:
	GlobusFileSystem(XrdSysLogger *lp, const char *configfn, XrdOucEnv *envP);
	virtual ~GlobusFileSystem();

	virtual bool Config(XrdSysLogger *lp, const char *configfn);

	XrdOssDF *newDir(const char *user = 0);
	XrdOssDF *newFile(const char *user = 0);

	// Inherit all other methods from HTTPFileSystem
	// Override only what's needed for Globus-specific functionality

	// Additional getter for Globus-specific token
	const TokenFile *getGlobusToken() const { return &m_globus_token; }
	
	// Getters for Globus-specific configuration
	const std::string &getGlobusEndpoint() const { return m_globus_endpoint; }
	const std::string &getGlobusCollectionId() const { return m_globus_collection_id; }

  protected:
	XrdSysError m_log;

	bool handle_required_config(const std::string &name_from_config,
								const char *desired_name,
								const std::string &source, std::string &target);

  private:
	// Globus-specific configuration
	std::string m_globus_endpoint;
	std::string m_globus_collection_id;
	TokenFile m_globus_token; // Additional token for Globus operations
}; 