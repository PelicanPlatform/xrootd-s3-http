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

#include <XrdOss/XrdOssDefaultSS.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <XrdVersion.hh>

#include <chrono>
#include <thread>

/**
 * Dummy OSS plugin that intentionally stalls on Stat operations.
 * Used for testing deadlock detection.
 */
class DummyStallOss : public XrdOss {
  public:
	DummyStallOss(XrdOss *oss) : m_oss(oss) {}

	virtual ~DummyStallOss() { delete m_oss; }

	XrdOssDF *newDir(const char *user = 0) override {
		return m_oss->newDir(user);
	}

	XrdOssDF *newFile(const char *user = 0) override {
		return m_oss->newFile(user);
	}

	int Stat(const char *path, struct stat *buff, int opts = 0,
			 XrdOucEnv *env = 0) override {
		// Stall for 10 seconds to trigger deadlock detection
		std::this_thread::sleep_for(std::chrono::seconds(10));
		return m_oss->Stat(path, buff, opts, env);
	}

	// Forward all other operations without stalling
	int Chmod(const char *path, mode_t mode, XrdOucEnv *env = 0) override {
		return m_oss->Chmod(path, mode, env);
	}

	int Create(const char *tid, const char *path, mode_t mode, XrdOucEnv &env,
			   int opts = 0) override {
		return m_oss->Create(tid, path, mode, env, opts);
	}

	int Mkdir(const char *path, mode_t mode, int mkpath = 0,
			  XrdOucEnv *envP = 0) override {
		return m_oss->Mkdir(path, mode, mkpath, envP);
	}

	int Reloc(const char *tident, const char *path, const char *cgName,
			  const char *anchor = 0) override {
		return m_oss->Reloc(tident, path, cgName, anchor);
	}

	int Remdir(const char *path, int Opts = 0, XrdOucEnv *envP = 0) override {
		return m_oss->Remdir(path, Opts, envP);
	}

	int Rename(const char *oPath, const char *nPath, XrdOucEnv *oEnvP = 0,
			   XrdOucEnv *nEnvP = 0) override {
		return m_oss->Rename(oPath, nPath, oEnvP, nEnvP);
	}

	int StatFS(const char *path, char *buff, int &blen,
			   XrdOucEnv *env = 0) override {
		return m_oss->StatFS(path, buff, blen, env);
	}

	int StatLS(XrdOucEnv &env, const char *path, char *buff,
			   int &blen) override {
		return m_oss->StatLS(env, path, buff, blen);
	}

	int StatPF(const char *path, struct stat *buff, int opts) override {
		return m_oss->StatPF(path, buff, opts);
	}

	int StatPF(const char *path, struct stat *buff) override {
		return m_oss->StatPF(path, buff);
	}

	int StatVS(XrdOssVSInfo *vsP, const char *sname = 0,
			   int updt = 0) override {
		return m_oss->StatVS(vsP, sname, updt);
	}

	int StatXA(const char *path, char *buff, int &blen,
			   XrdOucEnv *env = 0) override {
		return m_oss->StatXA(path, buff, blen, env);
	}

	int StatXP(const char *path, unsigned long long &attr,
			   XrdOucEnv *env = 0) override {
		return m_oss->StatXP(path, attr, env);
	}

	int Truncate(const char *path, unsigned long long fsize,
				 XrdOucEnv *env = 0) override {
		return m_oss->Truncate(path, fsize, env);
	}

	int Unlink(const char *path, int Opts = 0, XrdOucEnv *env = 0) override {
		return m_oss->Unlink(path, Opts, env);
	}

	int Lfn2Pfn(const char *Path, char *buff, int blen) override {
		return m_oss->Lfn2Pfn(Path, buff, blen);
	}

	const char *Lfn2Pfn(const char *Path, char *buff, int blen,
						int &rc) override {
		return m_oss->Lfn2Pfn(Path, buff, blen, rc);
	}

  private:
	XrdOss *m_oss;
};

extern "C" {

XrdVERSIONINFO(XrdOssAddStorageSystem2, DummyStall);

XrdOss *XrdOssAddStorageSystem2(XrdOss *curr_oss, XrdSysLogger *logger,
								const char *config_fn, const char *parms,
								XrdOucEnv *envP) {
	return new DummyStallOss(curr_oss);
}
}
