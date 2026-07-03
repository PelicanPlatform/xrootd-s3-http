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

#include <XrdOss/XrdOssWrapper.hh>
#include <XrdSys/XrdSysError.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <XrdVersion.hh>

#include <chrono>
#include <cstring>
#include <memory>
#include <thread>

namespace {
// Stall long enough to exceed any reasonable deadlock timeout, but only for
// paths containing the marker "stall" so xrootd's own start-up operations are
// unaffected.
void maybeStall(const char *path) {
	if (path && strstr(path, "stall")) {
		std::this_thread::sleep_for(std::chrono::seconds(30));
	}
}
} // namespace

/**
 * File handle that stalls on Open for marked paths.
 */
class DummyStallFile : public XrdOssWrapDF {
  public:
	explicit DummyStallFile(std::unique_ptr<XrdOssDF> df)
		: XrdOssWrapDF(*df), m_df(std::move(df)) {}

	virtual ~DummyStallFile() {}

	int Open(const char *path, int Oflag, mode_t Mode,
			 XrdOucEnv &env) override {
		maybeStall(path);
		return wrapDF.Open(path, Oflag, Mode, env);
	}

  private:
	std::unique_ptr<XrdOssDF> m_df;
};

/**
 * Dummy OSS plugin that intentionally stalls on Stat and file Open for paths
 * containing the marker "stall".  Used by the integration tests to trigger
 * deadlock detection on demand.  Everything else is forwarded unchanged (via
 * XrdOssWrapper).
 */
class DummyStallOss : public XrdOssWrapper {
  public:
	explicit DummyStallOss(XrdOss *oss) : XrdOssWrapper(*oss), m_oss(oss) {}

	virtual ~DummyStallOss() { delete m_oss; }

	XrdOssDF *newFile(const char *user = 0) override {
		return new DummyStallFile(
			std::unique_ptr<XrdOssDF>(wrapPI.newFile(user)));
	}

	int Stat(const char *path, struct stat *buff, int opts = 0,
			 XrdOucEnv *env = 0) override {
		maybeStall(path);
		return wrapPI.Stat(path, buff, opts, env);
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
