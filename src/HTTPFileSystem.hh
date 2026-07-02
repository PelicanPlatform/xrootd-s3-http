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

#include "TokenFile.hh"

#include <XrdOss/XrdOss.hh>
#include <XrdOuc/XrdOucStream.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSys/XrdSysPthread.hh>
#include <XrdVersion.hh>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>

// How the remote server's directory listings are retrieved.
//   Http    - plain GET returning XRootD's HTML directory index.
//   Webdav  - WebDAV PROPFIND returning a multi-status XML document.
//   Unknown - not yet determined (the "auto" flavor before/while probing);
//             callers should optimistically try WebDAV and fall back to HTTP.
enum class RemoteFlavor { Unknown, Http, Webdav };

class HTTPFileSystem : public XrdOss {
  public:
	HTTPFileSystem(XrdSysLogger *lp, const char *configfn, XrdOucEnv *envP);
	virtual ~HTTPFileSystem();

	virtual bool Config(XrdSysLogger *lp, const char *configfn);

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
	int Unlink(const char *path, int Opts = 0, XrdOucEnv *env = 0);
	int Lfn2Pfn(const char *Path, char *buff, int blen) { return -ENOSYS; }
	const char *Lfn2Pfn(const char *Path, char *buff, int blen, int &rc) {
		return nullptr;
	}

	const std::string &getHTTPHostName() const { return http_host_name; }
	const std::string &getHTTPHostUrl() const { return http_host_url; }
	const std::string &getHTTPUrlBase() const { return m_url_base; }
	const std::string &getStoragePrefix() const { return m_storage_prefix; }
	const std::string &getRemoteFlavor() const { return m_remote_flavor; }
	const TokenFile *getToken() const { return &m_token; }

	// Return the resolved remote flavor used to list directories.
	//
	// For the explicitly-configured "http"/"webdav" flavors this is a direct
	// mapping.  For "auto" it reflects the result of an OPTIONS probe; if the
	// probe has not yet succeeded (e.g. the remote was down at startup) this
	// returns RemoteFlavor::Unknown and lazily re-probes on a fixed interval.
	RemoteFlavor getResolvedFlavor();

  protected:
	XrdSysError m_log;

	bool handle_required_config(const std::string &name_from_config,
								const char *desired_name,
								const std::string &source, std::string &target);

  private:
	// Send an OPTIONS request to the remote and classify it as WebDAV-capable
	// (advertises PROPFIND) or plain HTTP.  Returns RemoteFlavor::Unknown if
	// the probe could not be completed.
	RemoteFlavor detectRemoteFlavor();

	// For the "auto" flavor: run detectRemoteFlavor() if the flavor is still
	// unknown and we have not probed within the retry interval.  Safe to call
	// concurrently and never throws.
	void maybeProbeFlavor();

	std::string http_host_name;
	std::string http_host_url;
	std::string m_url_base;
	std::string m_storage_prefix;
	std::string m_remote_flavor; // configured flavor: http, webdav or auto
	TokenFile m_token;

	// Resolved listing flavor; only meaningful (and mutated) for "auto".
	std::atomic<RemoteFlavor> m_resolved_flavor{RemoteFlavor::Unknown};
	std::mutex m_probe_mtx; // serializes OPTIONS probes for "auto"
	bool m_probed_once{false};
	std::chrono::steady_clock::time_point m_last_probe;
	// How long to wait before re-probing a remote that was unreachable.
	static constexpr std::chrono::seconds m_probe_interval{60};
};
