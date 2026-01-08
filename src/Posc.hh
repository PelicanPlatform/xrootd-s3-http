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

#ifndef __POSC_HH_
#define __POSC_HH_

#include "logging.hh"

#include <XrdOss/XrdOssWrapper.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSec/XrdSecEntityAttr.hh>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string_view>

// Forward declarations
class XrdSysError;
class XrdOucEnv;

class PoscFileSystem final : public XrdOssWrapper {
  public:
	PoscFileSystem(XrdOss *oss, std::unique_ptr<XrdSysError> log,
				   const char *configName, XrdOucEnv *envP);

	PoscFileSystem(XrdOss *oss, std::unique_ptr<XrdSysError> log,
				   const std::string &posc_dir,
				   XrdHTTPServer::LogMask log_mask);

	virtual ~PoscFileSystem();

	bool Config(const char *configfn);

	XrdOssDF *newDir(const char *user = 0) override;
	XrdOssDF *newFile(const char *user = 0) override;

	virtual int Chmod(const char *path, mode_t mode,
					  XrdOucEnv *env = 0) override;
	virtual int Create(const char *tid, const char *path, mode_t mode,
					   XrdOucEnv &env, int opts = 0) override;

	// Expire all old/stale files in the POSC directory.
	//
	// Not intended to be called directly except by unit tests.
	void ExpireFiles();

	// Generate a POSC filename for a given path.
	// The resulting filename will be within the POSC directory and will
	// have a high chance for being unique; unlike mkstemp however, there's
	// no guarantee provided for uniqueness.
	std::string GeneratePoscFile(const char *path, XrdOucEnv &env);

	bool InPoscDir(const std::filesystem::path &path) const;

	void InitPosc();

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

	std::pair<bool, std::string> SanitizePrefix(const std::filesystem::path &);

	static void SetFileTimeout(std::chrono::steady_clock::duration timeout) {
		m_posc_file_timeout = timeout;
	}

  private:
	static void ExpireThread(PoscFileSystem *fs);

	void ExpireUserFiles(XrdOucEnv &env);

	// Invoked on the shutdown of the library, will trigger the background
	// threads to wrap up and have a clean exit.
	static void Shutdown() __attribute__((destructor));

	// Verify the path is not inside the special "posc" directory
	// where temporary files are stored.  If not, call the provided
	// function with the extra arguments.
	template <class Fn, class... Args>
	int VerifyPath(std::string_view path, Fn &&fn, Args &&...args);

	// The location where temporary files are stored while they are being
	// written.
	std::filesystem::path m_posc_dir;

	// The underlying storage system we are wrapping.
	XrdOss *m_oss;
	std::unique_ptr<XrdSysError> m_log;

	// How long a file can be open and idle before it is considered stale
	// and is subject to deletion.
	static std::chrono::steady_clock::duration m_posc_file_timeout;

	// Static members to manage the periodic cleanup of stale files.
	static std::once_flag m_expiry_launch;
	static std::mutex m_shutdown_lock;
	static std::condition_variable m_shutdown_complete_cv;
	static std::condition_variable m_shutdown_requested_cv;
	static bool m_shutdown_requested;
	static bool m_shutdown_complete;
};

class PoscFile final : public XrdOssWrapDF {
  public:
	PoscFile(std::unique_ptr<XrdOssDF> wrapDF, XrdSysError &log, XrdOss &oss,
			 PoscFileSystem &posc_fs)
		: XrdOssWrapDF(*wrapDF), m_wrapped(std::move(wrapDF)), m_log(log),
		  m_oss(oss), m_posc_fs(posc_fs) {}

	virtual ~PoscFile();

	virtual int Close(long long *retsz = 0) override;

	// Returns the name of the temporary file that will be persisted on close or
	// the empty string if the file is not being created.
	//
	// Primarily this accessor is intended for unit tests.
	const std::string &GetPoscFilename() const { return m_posc_filename; }

	virtual int Open(const char *path, int Oflag, mode_t Mode,
					 XrdOucEnv &env) override;

	virtual ssize_t pgWrite(void *buffer, off_t offset, size_t wrlen,
							uint32_t *csvec, uint64_t opts) override;

	virtual int pgWrite(XrdSfsAio *aioparm, uint64_t opts) override;

	static void
	SetFileUpdateDuration(std::chrono::steady_clock::duration duration) {
		m_posc_file_update = duration;
	}

	// Iterate through all the open PoscFile instances and update their
	// mtime to prevent them from being deleted by the periodic cleanup
	// of stale/abandoned file handles in the POSC directory.
	static void UpdateOpenFiles();

	virtual ssize_t Write(const void *buffer, off_t offset,
						  size_t size) override;

	virtual int Write(XrdSfsAio *aiop) override;

  private:
	class XrdSecEntityAttrCopy final : public XrdSecEntityAttrCB {
	  public:
		XrdSecEntityAttrCopy(XrdSecEntityAttr &dest) : m_dest(dest) {}

		virtual XrdSecEntityAttrCB::Action Attr(const char *key,
												const char *val) override {
			m_dest.Add(key, val);
			return XrdSecEntityAttrCB::Action::Next;
		}

	  private:
		XrdSecEntityAttr &m_dest;
	};
	void CopySecEntity(const XrdSecEntity &in);

	mode_t m_posc_mode{0};
	std::unique_ptr<XrdOssDF> m_wrapped;
	std::unique_ptr<XrdOucEnv> m_posc_env;
	std::unique_ptr<XrdSecEntity> m_posc_entity;
	XrdSysError &m_log;
	XrdOss &m_oss;
	PoscFileSystem &m_posc_fs;
	std::atomic<std::chrono::system_clock::duration::rep> m_posc_mtime{0};
	std::string m_posc_filename;
	std::string m_orig_filename;
	std::string m_parent_to_create; // Parent directory to create on close
									// (empty if exists)
	off_t m_expected_size{
		-1}; // Expected file size from oss.asize, -1 if unknown

	// Static members to keep track of all PoscFile instances.
	// Periodically, the filesystem object will iterate through
	// this list and update the mtime of all the open files to
	// prevent them from being deleted.
	static std::mutex m_list_mutex;
	static PoscFile *m_first;
	PoscFile *m_next{nullptr};
	PoscFile *m_prev{nullptr};

	// How long a file can be open and idle before we update its mtime
	// to prevent it from being deleted.
	static std::chrono::steady_clock::duration m_posc_file_update;
};

class PoscDir final : public XrdOssWrapDF {
  public:
	PoscDir(std::unique_ptr<XrdOssDF> wrapDF, XrdSysError &log,
			PoscFileSystem &posc_fs)
		: XrdOssWrapDF(*wrapDF), m_wrapped(std::move(wrapDF)), m_log(log),
		  m_posc_fs(posc_fs) {}

	virtual ~PoscDir();

	virtual int Opendir(const char *path, XrdOucEnv &env) override;
	virtual int Readdir(char *buff, int blen) override;
	virtual int StatRet(struct stat *buff) override;
	virtual int Close(long long *retsz = 0) override;

  private:
	// The stat object provided by the caller which must be filled during
	// the Readdir on success.
	struct stat *m_stat_external{nullptr};
	struct stat m_stat;
	std::unique_ptr<XrdOssDF> m_wrapped;
	XrdSysError &m_log;
	PoscFileSystem &m_posc_fs;
	std::filesystem::path m_prefix;
};

#endif // __POSC_HH_
