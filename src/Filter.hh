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
#include <XrdVersion.hh>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

class XrdOucEnv;
class XrdSecEntity;
class XrdSysError;

//
// A filesystem wrapper which only permits accessing specific paths.
//
// For example, if the filter is "/foo/*.txt", then the underlying
// path /foo/test.txt will be accessible but the paths /bar.txt and
// /foo/test.csv will result in an ENOENT.
//
class FilterFileSystem final : public XrdOssWrapper {
  public:
	struct glob {
		bool m_match_dotfile{false};
		std::filesystem::path m_glob;
	};

	FilterFileSystem(XrdOss *oss, XrdSysLogger *log, const char *configName,
					 XrdOucEnv *envP);

	virtual ~FilterFileSystem();

	bool Config(const char *configfn);

	XrdOssDF *newDir(const char *user = 0) override;
	XrdOssDF *newFile(const char *user = 0) override;

	virtual int Chmod(const char *path, mode_t mode,
					  XrdOucEnv *env = 0) override;
	virtual int Create(const char *tid, const char *path, mode_t mode,
					   XrdOucEnv &env, int opts = 0) override;
	virtual int Mkdir(const char *path, mode_t mode, int mkpath = 0,
					  XrdOucEnv *envP = 0) override;
	virtual int Reloc(const char *tident, const char *path, const char *cgName,
					  const char *anchor = 0) override;
	virtual int Remdir(const char *path, int Opts = 0,
					   XrdOucEnv *envP = 0) override;
	virtual int Rename(const char *oPath, const char *nPath,
					   XrdOucEnv *oEnvP = 0, XrdOucEnv *nEnvP = 0) override;
	virtual int Stat(const char *path, struct stat *buff, int opts = 0,
					 XrdOucEnv *env = 0) override;
	virtual int StatFS(const char *path, char *buff, int &blen,
					   XrdOucEnv *env = 0) override;
	virtual int StatLS(XrdOucEnv &env, const char *path, char *buff,
					   int &blen) override;
	virtual int StatPF(const char *path, struct stat *buff, int opts) override;
	virtual int StatPF(const char *path, struct stat *buff) override;
	virtual int StatVS(XrdOssVSInfo *vsP, const char *sname = 0,
					   int updt = 0) override;
	virtual int StatXA(const char *path, char *buff, int &blen,
					   XrdOucEnv *env = 0) override;
	virtual int StatXP(const char *path, unsigned long long &attr,
					   XrdOucEnv *env = 0) override;
	virtual int Truncate(const char *path, unsigned long long fsize,
						 XrdOucEnv *env = 0) override;
	virtual int Unlink(const char *path, int Opts = 0,
					   XrdOucEnv *env = 0) override;
	virtual int Lfn2Pfn(const char *Path, char *buff, int blen) override;
	virtual const char *Lfn2Pfn(const char *Path, char *buff, int blen,
								int &rc) override;

	bool Glob(const char *path, bool &partial);
	bool Glob(std::string_view path, bool &partial);
	bool Glob(const std::filesystem::path &path, bool &partial);
	bool GlobOne(const std::filesystem::path &path, const glob &glob,
				 bool &partial);

  private:
	template <class Fn, class... Args>
	int VerifyPath(std::string_view path, bool partial_ok, Fn &&fn,
				   Args &&...args);

	std::vector<glob> m_globs;
	std::unique_ptr<XrdOss> m_oss;
	XrdSysError m_log;
};

class FilterFile final : public XrdOssWrapDF {
  public:
	FilterFile(std::unique_ptr<XrdOssDF> wrapDF, XrdSysError &log,
			   FilterFileSystem &oss)
		: XrdOssWrapDF(*wrapDF), m_wrapped(std::move(wrapDF)), m_log(log),
		  m_oss(oss) {}

	virtual ~FilterFile();

	int Open(const char *path, int Oflag, mode_t Mode, XrdOucEnv &env) override;

  private:
	std::unique_ptr<XrdOssDF> m_wrapped;
	XrdSysError &m_log;
	FilterFileSystem &m_oss;
};

class FilterDir final : public XrdOssWrapDF {
  public:
	FilterDir(std::unique_ptr<XrdOssDF> wrapDF, XrdSysError &log,
			  FilterFileSystem &oss)
		: XrdOssWrapDF(*wrapDF), m_wrapped(std::move(wrapDF)), m_log(log),
		  m_oss(oss) {}

	virtual ~FilterDir();

	virtual int Opendir(const char *path, XrdOucEnv &env) override;
	virtual int Readdir(char *buff, int blen) override;
	virtual int StatRet(struct stat *buff) override;
	virtual int Close(long long *retsz = 0) override;

  private:
	bool m_stat_avail{false};
	struct stat m_stat;
	std::unique_ptr<XrdOssDF> m_wrapped;
	XrdSysError &m_log;
	FilterFileSystem &m_oss;
	std::filesystem::path m_prefix;
};
