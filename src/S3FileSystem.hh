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

#pragma once

#include "S3AccessInfo.hh"
#include <XrdOss/XrdOss.hh>
#include <XrdOuc/XrdOucStream.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdVersion.hh>

#include <map>
#include <memory>
#include <string>

class S3FileSystem : public XrdOss {
  public:
	S3FileSystem(XrdSysLogger *lp, const char *configfn, XrdOucEnv *envP);
	virtual ~S3FileSystem();

	bool Config(XrdSysLogger *lp, const char *configfn);

	XrdOssDF *newDir(const char *user = 0);
	XrdOssDF *newFile(const char *user = 0);

	int Chmod(const char *path, mode_t mode, XrdOucEnv *env = 0) {
		return -ENOSYS;
	}
	void Connect(XrdOucEnv &env) {}
	int Create(const char *tid, const char *path, mode_t mode, XrdOucEnv &env,
			   int opts = 0);
	void Disc(XrdOucEnv &env) {}
	void EnvInfo(XrdOucEnv *env) {}
	uint64_t Features() { return 0; }
	int FSctl(int cmd, int alen, const char *args, char **resp = 0) {
		return -ENOSYS;
	}
	int Init(XrdSysLogger *lp, const char *cfn) { return 0; }
	int Init(XrdSysLogger *lp, const char *cfn, XrdOucEnv *en) { return 0; }
	int Mkdir(const char *path, mode_t mode, int mkpath = 0,
			  XrdOucEnv *env = 0) {
		return -ENOSYS;
	}
	int Reloc(const char *tident, const char *path, const char *cgName,
			  const char *anchor = 0) {
		return -ENOSYS;
	}
	int Remdir(const char *path, int Opts = 0, XrdOucEnv *env = 0) {
		return -ENOSYS;
	}
	int Rename(const char *oPath, const char *nPath, XrdOucEnv *oEnvP = 0,
			   XrdOucEnv *nEnvP = 0) {
		return -ENOSYS;
	}
	int Stat(const char *path, struct stat *buff, int opts = 0,
			 XrdOucEnv *env = 0);
	int Stats(char *buff, int blen) { return -ENOSYS; }
	int StatFS(const char *path, char *buff, int &blen, XrdOucEnv *env = 0) {
		return -ENOSYS;
	}
	int StatLS(XrdOucEnv &env, const char *path, char *buff, int &blen) {
		return -ENOSYS;
	}
	int StatPF(const char *path, struct stat *buff, int opts) {
		return -ENOSYS;
	}
	int StatPF(const char *path, struct stat *buff) { return -ENOSYS; }
	int StatVS(XrdOssVSInfo *vsP, const char *sname = 0, int updt = 0) {
		return -ENOSYS;
	}
	int StatXA(const char *path, char *buff, int &blen, XrdOucEnv *env = 0) {
		return -ENOSYS;
	}
	int StatXP(const char *path, unsigned long long &attr, XrdOucEnv *env = 0) {
		return -ENOSYS;
	}
	int Truncate(const char *path, unsigned long long fsize,
				 XrdOucEnv *env = 0) {
		return -ENOSYS;
	}
	int Unlink(const char *path, int Opts = 0, XrdOucEnv *env = 0) {
		return -ENOSYS;
	}
	int Lfn2Pfn(const char *Path, char *buff, int blen) { return -ENOSYS; }
	const char *Lfn2Pfn(const char *Path, char *buff, int blen, int &rc) {
		return nullptr;
	}

	// Given a path as seen by XRootD, split it into the configured prefix and
	// the object within the prefix.
	//
	// The returned `exposedPath` can be later used with the `get*` functions to
	// fetch the required S3 configuration.
	int parsePath(const char *fullPath, std::string &exposedPath,
				  std::string &object) const;

	bool exposedPathExists(const std::string &exposedPath) const {
		return m_s3_access_map.count(exposedPath) > 0;
	}
	const std::string &getS3ServiceName(const std::string &exposedPath) const {
		return m_s3_access_map.at(exposedPath)->getS3ServiceName();
	}
	const std::string &getS3Region(const std::string &exposedPath) const {
		return m_s3_access_map.at(exposedPath)->getS3Region();
	}
	const std::string &getS3ServiceURL(const std::string &exposedPath) const {
		return m_s3_access_map.at(exposedPath)->getS3ServiceUrl();
	}
	const std::string &getS3BucketName(const std::string &exposedPath) const {
		return m_s3_access_map.at(exposedPath)->getS3BucketName();
	}
	const std::string &
	getS3AccessKeyFile(const std::string &exposedPath) const {
		return m_s3_access_map.at(exposedPath)->getS3AccessKeyFile();
	}
	const std::string &
	getS3SecretKeyFile(const std::string &exposedPath) const {
		return m_s3_access_map.at(exposedPath)->getS3SecretKeyFile();
	}
	const std::string &getS3URLStyle() const { return s3_url_style; }

	const std::shared_ptr<S3AccessInfo>
	getS3AccessInfo(const std::string &exposedPath, std::string &object) const;

  private:
	XrdOucEnv *m_env;
	XrdSysError m_log;

	bool handle_required_config(const char *desired_name,
								const std::string &source);
	std::map<std::string, std::shared_ptr<S3AccessInfo>> m_s3_access_map;
	std::string s3_url_style;

};
