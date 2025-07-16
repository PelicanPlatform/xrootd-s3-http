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

#include "TokenFile.hh"

#include <XrdOss/XrdOssWrapper.hh>
#include <XrdOuc/XrdOucStream.hh>
#include <XrdSys/XrdSysError.hh>

#include <memory>
#include <string>

class GlobusFileSystem : public XrdOssWrapper {
  public:
	GlobusFileSystem(XrdOss *oss, XrdSysLogger *lp, const char *configfn,
					 XrdOucEnv *envP);
	virtual ~GlobusFileSystem();

	virtual bool Config(XrdSysLogger *lp, const char *configfn);

	XrdOssDF *newDir(const char *user = 0) override;
	XrdOssDF *newFile(const char *user = 0) override;
	int Chmod(const char *path, mode_t mode, XrdOucEnv *env = 0) override {
		return -ENOSYS;
	}
	int Rename(const char *oPath, const char *nPath, XrdOucEnv *oEnvP = 0,
			   XrdOucEnv *nEnvP = 0) override {
		return -ENOSYS;
	}
	int Stat(const char *path, struct stat *buff, int opts = 0,
			 XrdOucEnv *env = 0);
	int StatFS(const char *path, char *buff, int &blen,
			   XrdOucEnv *env = 0) override {
		return -ENOSYS;
	}
	int StatLS(XrdOucEnv &env, const char *path, char *buff,
			   int &blen) override {
		return -ENOSYS;
	}
	int StatPF(const char *path, struct stat *buff, int opts) override {
		return -ENOSYS;
	}
	int StatPF(const char *path, struct stat *buff) override { return -ENOSYS; }
	int StatVS(XrdOssVSInfo *vsP, const char *sname = 0,
			   int updt = 0) override {
		return -ENOSYS;
	}
	int StatXA(const char *path, char *buff, int &blen,
			   XrdOucEnv *env = 0) override {
		return -ENOSYS;
	}
	int StatXP(const char *path, unsigned long long &attr,
			   XrdOucEnv *env = 0) override {
		return -ENOSYS;
	}
	int Truncate(const char *path, unsigned long long fsize,
				 XrdOucEnv *env = 0) override {
		return -ENOSYS;
	}
	int Unlink(const char *path, int Opts = 0, XrdOucEnv *env = 0) override {
		return -ENOSYS;
	}

	// Getters for Globus-specific configuration
	const std::string &getStoragePrefix() const { return m_storage_prefix; }
	const TokenFile *getTransferToken() const { return &m_transfer_token; }

	// Methods to get operation-specific URLs
	const std::string getLsUrl(const std::string &relative_path = "") const;
	const std::string getStatUrl(const std::string &relative_path = "") const;

	// Static utility method for parsing timestamps
	static time_t parseTimestamp(const std::string& last_modified);

  protected:
	bool handle_required_config(const std::string &name_from_config,
								const char *desired_name,
								const std::string &source, std::string &target);

  private:
	const std::string
	getOperationUrl(const std::string &operation,
					const std::string &relative_path = "") const;

	std::unique_ptr<XrdOss> m_oss;
	std::string m_object;
	XrdOucEnv *m_env;
	XrdSysError m_log;

	// Globus-specific configuration
	std::string m_transfer_url;
	std::string m_storage_prefix;
	TokenFile m_transfer_token;
};