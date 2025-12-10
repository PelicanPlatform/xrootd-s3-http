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

#ifndef __DEADLOCK_OSS_HH_
#define __DEADLOCK_OSS_HH_

#include "DeadlockDetector.hh"

#include <XrdOss/XrdOssWrapper.hh>

#include <memory>

// Forward declarations
class XrdSysError;
class XrdOucEnv;

/**
 * OSS wrapper that adds deadlock detection to all operations.
 *
 * Wraps another XrdOss implementation and creates a DeadlockMonitor
 * for each operation to detect if it blocks for too long.
 */
class DeadlockOss final : public XrdOssWrapper {
  public:
	DeadlockOss(XrdOss *oss, std::unique_ptr<XrdSysError> log,
				const char *configName, XrdOucEnv *envP);

	virtual ~DeadlockOss();

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

  private:
	XrdOss *m_oss;
	std::unique_ptr<XrdSysError> m_log;
};

/**
 * Directory wrapper with deadlock detection.
 */
class DeadlockOssDir final : public XrdOssWrapDF {
  public:
	DeadlockOssDir(std::unique_ptr<XrdOssDF> wrapDF)
		: XrdOssWrapDF(*wrapDF), m_wrapped(std::move(wrapDF)) {}

	virtual ~DeadlockOssDir();

	virtual int Opendir(const char *path, XrdOucEnv &env) override;
	virtual int Readdir(char *buff, int blen) override;
	virtual int StatRet(struct stat *buff) override;
	virtual int Close(long long *retsz = 0) override;

  private:
	std::unique_ptr<XrdOssDF> m_wrapped;
};

/**
 * File wrapper with deadlock detection.
 */
class DeadlockOssFile final : public XrdOssWrapDF {
  public:
	DeadlockOssFile(std::unique_ptr<XrdOssDF> wrapDF)
		: XrdOssWrapDF(*wrapDF), m_wrapped(std::move(wrapDF)) {}

	virtual ~DeadlockOssFile();

	virtual int Close(long long *retsz = 0) override;
	virtual int Open(const char *path, int Oflag, mode_t Mode,
					 XrdOucEnv &env) override;
	virtual ssize_t Read(void *buffer, off_t offset, size_t size) override;
	virtual int Read(XrdSfsAio *aiop) override;
	virtual ssize_t ReadRaw(void *buffer, off_t offset, size_t size) override;
	virtual ssize_t pgRead(void *buffer, off_t offset, size_t rdlen,
						   uint32_t *csvec, uint64_t opts) override;
	virtual int pgRead(XrdSfsAio *aioparm, uint64_t opts) override;
	virtual ssize_t pgWrite(void *buffer, off_t offset, size_t wrlen,
							uint32_t *csvec, uint64_t opts) override;
	virtual int pgWrite(XrdSfsAio *aioparm, uint64_t opts) override;
	virtual ssize_t Write(const void *buffer, off_t offset,
						  size_t size) override;
	virtual int Write(XrdSfsAio *aiop) override;
	virtual int Fstat(struct stat *buff) override;
	virtual int Fsync() override;
	virtual int Fsync(XrdSfsAio *aiop) override;
	virtual int Ftruncate(unsigned long long flen) override;
	virtual off_t getMmap(void **addr) override;
	virtual int isCompressed(char *cxidp = 0) override;

  private:
	std::unique_ptr<XrdOssDF> m_wrapped;
};

#endif // __DEADLOCK_OSS_HH_
