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

#include "DeadlockOss.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdSys/XrdSysError.hh>
#include <XrdVersion.hh>

DeadlockOss::DeadlockOss(XrdOss *oss, std::unique_ptr<XrdSysError> log,
						 const char *configName, XrdOucEnv *envP)
	: XrdOssWrapper(*oss), m_oss(oss), m_log(std::move(log)) {
	// Initialize the deadlock detector
	auto &detector = DeadlockDetector::GetInstance();
	if (!detector.Initialize(m_log.get(), configName)) {
		m_log->Emsg("DeadlockOss",
					"Failed to initialize deadlock detector, continuing "
					"without deadlock detection");
	}
}

DeadlockOss::~DeadlockOss() {}

XrdOssDF *DeadlockOss::newDir(const char *user) {
	std::unique_ptr<XrdOssDF> wrapped(m_oss->newDir(user));
	return new DeadlockOssDir(std::move(wrapped));
}

XrdOssDF *DeadlockOss::newFile(const char *user) {
	std::unique_ptr<XrdOssDF> wrapped(m_oss->newFile(user));
	return new DeadlockOssFile(std::move(wrapped));
}

int DeadlockOss::Chmod(const char *path, mode_t mode, XrdOucEnv *env) {
	DeadlockMonitor monitor("Chmod");
	return wrapPI.Chmod(path, mode, env);
}

int DeadlockOss::Create(const char *tid, const char *path, mode_t mode,
						XrdOucEnv &env, int opts) {
	DeadlockMonitor monitor("Create");
	return wrapPI.Create(tid, path, mode, env, opts);
}

int DeadlockOss::Mkdir(const char *path, mode_t mode, int mkpath,
					   XrdOucEnv *envP) {
	DeadlockMonitor monitor("Mkdir");
	return wrapPI.Mkdir(path, mode, mkpath, envP);
}

int DeadlockOss::Reloc(const char *tident, const char *path,
					   const char *cgName, const char *anchor) {
	DeadlockMonitor monitor("Reloc");
	return wrapPI.Reloc(tident, path, cgName, anchor);
}

int DeadlockOss::Remdir(const char *path, int Opts, XrdOucEnv *envP) {
	DeadlockMonitor monitor("Remdir");
	return wrapPI.Remdir(path, Opts, envP);
}

int DeadlockOss::Rename(const char *oPath, const char *nPath,
						XrdOucEnv *oEnvP, XrdOucEnv *nEnvP) {
	DeadlockMonitor monitor("Rename");
	return wrapPI.Rename(oPath, nPath, oEnvP, nEnvP);
}

int DeadlockOss::Stat(const char *path, struct stat *buff, int opts,
					  XrdOucEnv *env) {
	DeadlockMonitor monitor("Stat");
	return wrapPI.Stat(path, buff, opts, env);
}

int DeadlockOss::StatFS(const char *path, char *buff, int &blen,
						XrdOucEnv *env) {
	DeadlockMonitor monitor("StatFS");
	return wrapPI.StatFS(path, buff, blen, env);
}

int DeadlockOss::StatLS(XrdOucEnv &env, const char *path, char *buff,
						int &blen) {
	DeadlockMonitor monitor("StatLS");
	return wrapPI.StatLS(env, path, buff, blen);
}

int DeadlockOss::StatPF(const char *path, struct stat *buff, int opts) {
	DeadlockMonitor monitor("StatPF");
	return wrapPI.StatPF(path, buff, opts);
}

int DeadlockOss::StatPF(const char *path, struct stat *buff) {
	DeadlockMonitor monitor("StatPF");
	return wrapPI.StatPF(path, buff);
}

int DeadlockOss::StatVS(XrdOssVSInfo *vsP, const char *sname, int updt) {
	DeadlockMonitor monitor("StatVS");
	return wrapPI.StatVS(vsP, sname, updt);
}

int DeadlockOss::StatXA(const char *path, char *buff, int &blen,
						XrdOucEnv *env) {
	DeadlockMonitor monitor("StatXA");
	return wrapPI.StatXA(path, buff, blen, env);
}

int DeadlockOss::StatXP(const char *path, unsigned long long &attr,
						XrdOucEnv *env) {
	DeadlockMonitor monitor("StatXP");
	return wrapPI.StatXP(path, attr, env);
}

int DeadlockOss::Truncate(const char *path, unsigned long long fsize,
						  XrdOucEnv *env) {
	DeadlockMonitor monitor("Truncate");
	return wrapPI.Truncate(path, fsize, env);
}

int DeadlockOss::Unlink(const char *path, int Opts, XrdOucEnv *env) {
	DeadlockMonitor monitor("Unlink");
	return wrapPI.Unlink(path, Opts, env);
}

int DeadlockOss::Lfn2Pfn(const char *Path, char *buff, int blen) {
	DeadlockMonitor monitor("Lfn2Pfn");
	return wrapPI.Lfn2Pfn(Path, buff, blen);
}

const char *DeadlockOss::Lfn2Pfn(const char *Path, char *buff, int blen,
								 int &rc) {
	DeadlockMonitor monitor("Lfn2Pfn");
	return wrapPI.Lfn2Pfn(Path, buff, blen, rc);
}

// DeadlockOssDir implementation

DeadlockOssDir::~DeadlockOssDir() {}

int DeadlockOssDir::Opendir(const char *path, XrdOucEnv &env) {
	DeadlockMonitor monitor("Opendir");
	return wrapDF.Opendir(path, env);
}

int DeadlockOssDir::Readdir(char *buff, int blen) {
	DeadlockMonitor monitor("Readdir");
	return wrapDF.Readdir(buff, blen);
}

int DeadlockOssDir::StatRet(struct stat *buff) {
	DeadlockMonitor monitor("StatRet");
	return wrapDF.StatRet(buff);
}

int DeadlockOssDir::Close(long long *retsz) {
	DeadlockMonitor monitor("Close");
	return wrapDF.Close(retsz);
}

// DeadlockOssFile implementation

DeadlockOssFile::~DeadlockOssFile() {}

int DeadlockOssFile::Close(long long *retsz) {
	DeadlockMonitor monitor("Close");
	return wrapDF.Close(retsz);
}

int DeadlockOssFile::Open(const char *path, int Oflag, mode_t Mode,
						  XrdOucEnv &env) {
	DeadlockMonitor monitor("Open");
	return wrapDF.Open(path, Oflag, Mode, env);
}

ssize_t DeadlockOssFile::Read(void *buffer, off_t offset, size_t size) {
	DeadlockMonitor monitor("Read");
	return wrapDF.Read(buffer, offset, size);
}

int DeadlockOssFile::Read(XrdSfsAio *aiop) {
	DeadlockMonitor monitor("Read");
	return wrapDF.Read(aiop);
}

ssize_t DeadlockOssFile::ReadRaw(void *buffer, off_t offset, size_t size) {
	DeadlockMonitor monitor("ReadRaw");
	return wrapDF.ReadRaw(buffer, offset, size);
}

ssize_t DeadlockOssFile::pgRead(void *buffer, off_t offset, size_t rdlen,
								uint32_t *csvec, uint64_t opts) {
	DeadlockMonitor monitor("pgRead");
	return wrapDF.pgRead(buffer, offset, rdlen, csvec, opts);
}

int DeadlockOssFile::pgRead(XrdSfsAio *aioparm, uint64_t opts) {
	DeadlockMonitor monitor("pgRead");
	return wrapDF.pgRead(aioparm, opts);
}

ssize_t DeadlockOssFile::pgWrite(void *buffer, off_t offset, size_t wrlen,
								 uint32_t *csvec, uint64_t opts) {
	DeadlockMonitor monitor("pgWrite");
	return wrapDF.pgWrite(buffer, offset, wrlen, csvec, opts);
}

int DeadlockOssFile::pgWrite(XrdSfsAio *aioparm, uint64_t opts) {
	DeadlockMonitor monitor("pgWrite");
	return wrapDF.pgWrite(aioparm, opts);
}

ssize_t DeadlockOssFile::Write(const void *buffer, off_t offset, size_t size) {
	DeadlockMonitor monitor("Write");
	return wrapDF.Write(buffer, offset, size);
}

int DeadlockOssFile::Write(XrdSfsAio *aiop) {
	DeadlockMonitor monitor("Write");
	return wrapDF.Write(aiop);
}

int DeadlockOssFile::Fstat(struct stat *buff) {
	DeadlockMonitor monitor("Fstat");
	return wrapDF.Fstat(buff);
}

int DeadlockOssFile::Fsync() {
	DeadlockMonitor monitor("Fsync");
	return wrapDF.Fsync();
}

int DeadlockOssFile::Fsync(XrdSfsAio *aiop) {
	DeadlockMonitor monitor("Fsync");
	return wrapDF.Fsync(aiop);
}

int DeadlockOssFile::Ftruncate(unsigned long long flen) {
	DeadlockMonitor monitor("Ftruncate");
	return wrapDF.Ftruncate(flen);
}

off_t DeadlockOssFile::getMmap(void **addr) {
	DeadlockMonitor monitor("getMmap");
	return wrapDF.getMmap(addr);
}

int DeadlockOssFile::isCompressed(char *cxidp) {
	DeadlockMonitor monitor("isCompressed");
	return wrapDF.isCompressed(cxidp);
}

extern "C" {

XrdVERSIONINFO(XrdOssAddStorageSystem2, DeadlockOss);

XrdOss *XrdOssAddStorageSystem2(XrdOss *curr_oss, XrdSysLogger *logger,
								const char *config_fn, const char *parms,
								XrdOucEnv *envP) {
	std::unique_ptr<XrdSysError> log(new XrdSysError(logger, "deadlock_"));
	try {
		return new DeadlockOss(curr_oss, std::move(log), config_fn, envP);
	} catch (std::exception &e) {
		XrdSysError tmp_log(logger, "deadlock_");
		tmp_log.Emsg("Initialize",
					 "Encountered a runtime failure when initializing the "
					 "deadlock detection OSS:",
					 e.what());
		return nullptr;
	}
}
}
