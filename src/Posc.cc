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

#include "Posc.hh"
#include "logging.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucGatherConf.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSec/XrdSecEntityAttr.hh>
#include <XrdSys/XrdSysError.hh>
#include <XrdSys/XrdSysPlatform.hh>
#include <XrdVersion.hh>

#include <fcntl.h>
#include <functional>
#include <sstream>
#include <thread>

using namespace XrdHTTPServer;

PoscFile *PoscFile::m_first = nullptr;
std::mutex PoscFile::m_list_mutex;
std::chrono::steady_clock::duration PoscFile::m_posc_file_update =
	std::chrono::minutes(19);
std::chrono::steady_clock::duration PoscFileSystem::m_posc_file_timeout =
	std::chrono::hours(1);

std::once_flag PoscFileSystem::m_expiry_launch;
std::mutex PoscFileSystem::m_shutdown_lock;
std::condition_variable PoscFileSystem::m_shutdown_requested_cv;
std::condition_variable PoscFileSystem::m_shutdown_complete_cv;
bool PoscFileSystem::m_shutdown_requested = false;
bool PoscFileSystem::m_shutdown_complete =
	true; // Starts in "true" state as the thread hasn't started

PoscFileSystem::PoscFileSystem(XrdOss *oss, std::unique_ptr<XrdSysError> log,
							   const char *configName, XrdOucEnv *envP)
	: XrdOssWrapper(*oss), m_oss(oss), m_log(std::move(log)) {
	if (!Config(configName)) {
		m_log->Emsg("Initialize", "Failed to configure the POSC layer");
		throw std::runtime_error("Failed to configure the POSC layer");
	}
	InitPosc();
}

PoscFileSystem::PoscFileSystem(XrdOss *oss, std::unique_ptr<XrdSysError> log,
							   const std::string &posc_dir, LogMask log_mask)
	: XrdOssWrapper(*oss), m_oss(oss), m_log(std::move(log)) {
	m_log->setMsgMask(log_mask);
	InitPosc();
}

void PoscFileSystem::InitPosc() {
	std::call_once(m_expiry_launch, [&] {
		{
			std::unique_lock lock(m_shutdown_lock);
			if (m_shutdown_requested) {
				m_log->Emsg("Initialize",
							"POSC expiry thread already requested shutdown");
				return;
			}
			m_shutdown_complete = false;
		}
		std::thread t(PoscFileSystem::ExpireThread, this);
		t.detach();
	});

	m_log->Emsg("Initialize", "PoscFileSystem initialized");
}

PoscFileSystem::~PoscFileSystem() {}

int PoscFileSystem::Chmod(const char *path, mode_t mode, XrdOucEnv *env) {
	return VerifyPath(path, &XrdOss::Chmod, path, mode, env);
}

bool PoscFileSystem::Config(const char *configfn) {
	m_log->setMsgMask(LogMask::Error | LogMask::Warning);

	XrdOucGatherConf poscConf("posc.prefix posc.trace", m_log.get());
	int result;
	if ((result = poscConf.Gather(configfn, XrdOucGatherConf::trim_lines)) <
		0) {
		m_log->Emsg("Config", -result, "parsing config file", configfn);
		return false;
	}

	char *val;
	while (poscConf.GetLine()) {
		val = poscConf.GetToken();
		if (!strcmp(val, "trace")) {
			m_log->setMsgMask(0);
			if (!(val = poscConf.GetToken())) {
				m_log->Emsg("Config",
							"posc.trace requires an argument.  Usage: "
							"posc.trace [all|error|warning|info|debug|none]");
				return false;
			}
			do {
				if (!strcmp(val, "all")) {
					m_log->setMsgMask(m_log->getMsgMask() | LogMask::All);
				} else if (!strcmp(val, "error")) {
					m_log->setMsgMask(m_log->getMsgMask() | LogMask::Error);
				} else if (!strcmp(val, "warning")) {
					m_log->setMsgMask(m_log->getMsgMask() | LogMask::Error |
									  LogMask::Warning);
				} else if (!strcmp(val, "info")) {
					m_log->setMsgMask(m_log->getMsgMask() | LogMask::Error |
									  LogMask::Warning | LogMask::Info);
				} else if (!strcmp(val, "debug")) {
					m_log->setMsgMask(m_log->getMsgMask() | LogMask::Error |
									  LogMask::Warning | LogMask::Info |
									  LogMask::Debug);
				} else if (!strcmp(val, "none")) {
					m_log->setMsgMask(0);
				}
			} while ((val = poscConf.GetToken()));
		} else if (!strcmp(val, "prefix")) {
			if (!(val = poscConf.GetToken())) {
				m_log->Emsg("Config", "posc.prefix requires an argument.  "
									  "Usage: posc.prefix posc_directory");
				return false;
			}
			m_posc_dir = std::filesystem::path(val);
			if (!m_posc_dir.is_absolute()) {
				m_log->Emsg("Config",
							"posc.prefix requires an absolute path.  Usage: "
							"posc.prefix posc_directory");
				return false;
			}
		} else {
			m_log->Emsg("Config", "Unknown configuration directive", val);
			return false;
		}
	}
	if (m_posc_dir.empty()) {
		m_log->Emsg("Config",
					"No POSC temporary directory specified but is required. "
					"Usage: posc.prefix posc_directory");
		return false;
	}
	struct stat sb;
	int rv;
	if ((rv = m_oss->Stat(m_posc_dir.c_str(), &sb, 0, nullptr)) != 0) {
		if (rv == -ENOENT) {
			m_log->Emsg("Config", "POSC directory does not exist",
						m_posc_dir.c_str());
			if ((rv = m_oss->Mkdir(m_posc_dir.c_str(), 0755, 1, nullptr)) !=
				0) {
				m_log->Emsg("Config", "Failed to create POSC directory",
							m_posc_dir.c_str(), strerror(-rv));
				return false;
			}
			m_log->Emsg("Config", "Created POSC directory", m_posc_dir.c_str());
			sb.st_mode = 0755 | S_IFDIR;
		} else {
			m_log->Emsg("Config",
						"POSC directory does not exist or is not accessible",
						m_posc_dir.c_str());
			return false;
		}
	}
	if (!S_ISDIR(sb.st_mode)) {
		m_log->Emsg("Config", "POSC directory is not a directory",
					m_posc_dir.c_str());
		return false;
	}
	return true;
}

int PoscFileSystem::Create(const char *tid, const char *path, mode_t mode,
						   XrdOucEnv &env, int opts) {
	// The open flags are passed in opts >> 8. If O_CREAT or O_TRUNC are set,
	// POSC will handle the file creation in Open(), so we should NOT create
	// the file here at the final destination. This prevents an empty file
	// from appearing in the exported directory during upload.
	int open_flags = opts >> 8;
	if (open_flags & (O_CREAT | O_TRUNC)) {
		m_log->Log(LogMask::Debug, "POSC",
				   "Skipping Create for POSC-handled file:", path);
		return 0;
	}
	return VerifyPath(path, &XrdOss::Create, tid, path, mode, env, opts);
}

void PoscFileSystem::ExpireFiles() {
	auto dp_raw = wrapPI.newDir(m_posc_dir.c_str());
	if (!dp_raw) {
		m_log->Emsg("ExpireFiles",
					"Failed to create directory object for POSC directory");
		return;
	}
	std::unique_ptr<XrdOssDF> dp(dp_raw);
	XrdOucEnv env;
	if (dp->Opendir(m_posc_dir.c_str(), env) != 0) {
		m_log->Emsg("ExpireFiles", "Failed to open POSC directory",
					m_posc_dir.c_str());
		return;
	}

	int rv;
	char buff[MAXPATHLEN];
	struct stat sb;
	bool supportsStatRet = dp->StatRet(&sb) == 0;
	while ((rv = dp->Readdir(buff, sizeof(buff))) == 0) {
		if (buff[0] == '\0') {
			// No more entries
			break;
		}
		if (!strcmp(buff, ".") || !strcmp(buff, "..") || buff[0] == '.') {
			// Skip current, parent, and hidden directory entries
			continue;
		}

		XrdSecEntity secEnt = {};
		secEnt.name = buff;
		XrdOucEnv userEnv(nullptr, 0, &secEnt);

		if (supportsStatRet) {
			if (sb.st_mode & S_IFDIR) {
				ExpireUserFiles(userEnv);
			}
		} else {
			auto destPath = m_posc_dir / buff;
			rv = wrapPI.Stat(destPath.c_str(), &sb, 0, &userEnv);
			if (rv == 0) {
				if (sb.st_mode & S_IFDIR) {
					ExpireUserFiles(userEnv);
				}
			} else if (m_log->getMsgMask() & LogMask::Warning) {
				std::stringstream ss;
				ss << "Failed to stat " << destPath.c_str()
				   << " when scanning POSC directory: " << strerror(errno);
				m_log->Log(LogMask::Warning, "ExpireFiles", ss.str().c_str());
			}
		}
	}
	if (rv) {
		m_log->Emsg("ExpireFiles", "Error reading POSC directory",
					m_posc_dir.c_str(), strerror(-rv));
	}
	dp->Close();
}

void PoscFileSystem::ExpireThread(PoscFileSystem *fs) {
	while (true) {
		{
			std::unique_lock lock(m_shutdown_lock);
			m_shutdown_requested_cv.wait_for(lock, std::chrono::seconds(5), [] {
				return m_shutdown_requested;
			});
			if (m_shutdown_requested) {
				break;
			}
		}

		PoscFile::UpdateOpenFiles();

		fs->ExpireFiles();
	}
	std::unique_lock lock(m_shutdown_lock);
	m_shutdown_complete = true;
	m_shutdown_complete_cv.notify_one();
}

void PoscFileSystem::ExpireUserFiles(XrdOucEnv &env) {
	if (!env.secEnv() || !env.secEnv()->name || (*env.secEnv()->name) == '\0') {
		m_log->Log(LogMask::Debug, "ExpireUserFiles",
				   "Skipping expiry for anonymous or invalid user");
		return;
	}
	auto user_posc_dir = m_posc_dir / env.secEnv()->name;
	m_log->Log(LogMask::Debug, "Expiring all files inside directory",
			   user_posc_dir.c_str());

	auto dp_raw = wrapPI.newDir(user_posc_dir.c_str());
	if (!dp_raw) {
		m_log->Emsg("ExpireUserFiles",
					"Failed to create directory object for POSC user directory",
					user_posc_dir.c_str());
		return;
	}
	std::unique_ptr<XrdOssDF> dp(dp_raw);
	if (dp->Opendir(user_posc_dir.c_str(), env) != 0) {
		m_log->Emsg("ExpireUserFiles", "Failed to open POSC user directory",
					user_posc_dir.c_str());
		return;
	}
	int rv;
	char buff[NAME_MAX];
	auto oldest_acceptable =
		std::chrono::system_clock::now() - m_posc_file_timeout;
	struct stat sb;
	memset(&sb, '\0', sizeof(sb));
	auto supportsStatRet = dp->StatRet(&sb) == 0;
	while ((rv = dp->Readdir(buff, NAME_MAX)) == 0) {
		if (buff[0] == '\0') {
			// No more entries
			break;
		}
		if (strncmp(buff, "in_progress.", strlen("in_progress."))) {
			// Skip current, parent, and hidden directory entries
			continue;
		}
		if (!supportsStatRet) {
			auto destPath = user_posc_dir / buff;
			rv = wrapPI.Stat(destPath.c_str(), &sb, 0, &env);
			if (rv) {
				m_log->Log(LogMask::Warning, "ExpireUserFiles",
						   "Failed to stat POSC file", destPath.c_str(),
						   strerror(-rv));
				continue;
			}
		}

		if (sb.st_mode & S_IFDIR) {
			// Skip directories
			continue;
		}
#ifdef __APPLE__
		struct timespec file_mtime = sb.st_mtimespec;
		file_mtime.tv_sec = sb.st_mtimespec.tv_sec;
#else
		struct timespec file_mtime = sb.st_mtim;
#endif
		auto file_mtime_tp = std::chrono::system_clock::time_point(
			std::chrono::duration_cast<std::chrono::system_clock::duration>(
				std::chrono::seconds(file_mtime.tv_sec) +
				std::chrono::nanoseconds(file_mtime.tv_nsec)));
		if (file_mtime_tp >= oldest_acceptable) {
			// File is still in use, skip it
			continue;
		}

		// File is stale, remove it
		auto full_path = user_posc_dir / buff;
		if ((rv = wrapPI.Unlink(full_path.c_str(), 0, &env)) != 0) {
			m_log->Emsg("ExpireUserFiles", "Failed to remove stale POSC file",
						full_path.c_str(), strerror(-rv));
			continue;
		}
		m_log->Log(LogMask::Debug, "POSC", "Removed stale POSC file",
				   full_path.c_str());
	}
	if (rv) {
		m_log->Emsg("ExpireFiles", "Error reading POSC directory",
					m_posc_dir.c_str(), strerror(-rv));
	}
	dp->Close();
}

bool PoscFileSystem::InPoscDir(const std::filesystem::path &path) const {
	auto path_iter = path.begin();
	for (auto posc_dir_iter = m_posc_dir.begin();
		 posc_dir_iter != m_posc_dir.end(); ++posc_dir_iter, ++path_iter) {
		// The path has fewer components than our storage directory; hence it is
		// not contained inside.
		if (path_iter == path.end()) {
			return false;
		}
		if (*posc_dir_iter != *path_iter) {
			return false;
		}
	}
	// In this case, the path has more components than the POSC directory and
	// all of the components match, so it is inside.
	return true;
}

std::string PoscFileSystem::GeneratePoscFile(const char *path, XrdOucEnv &env) {
	std::filesystem::path posc_filename = m_posc_dir;
	if (env.secEnv() && env.secEnv()->name && (*env.secEnv()->name) != '\0') {
		posc_filename /= env.secEnv()->name;
	} else {
		posc_filename /= "anonymous";
	}
	posc_filename /= "in_progress." + std::to_string(time(NULL)) + "." +
					 std::to_string(rand() % 1000000);

	return posc_filename.string();
}

int PoscFileSystem::Lfn2Pfn(const char *Path, char *buff, int blen) {
	return VerifyPath(Path,
					  static_cast<int (XrdOss::*)(const char *, char *, int)>(
						  &XrdOss::Lfn2Pfn),
					  Path, buff, blen);
}

const char *PoscFileSystem::Lfn2Pfn(const char *Path, char *buff, int blen,
									int &rc) {
	if (InPoscDir(Path)) {
		rc = -ENOENT;
		return nullptr;
	}
	return wrapPI.Lfn2Pfn(Path, buff, blen, rc);
}

int PoscFileSystem::Mkdir(const char *path, mode_t mode, int mkpath,
						  XrdOucEnv *envP) {
	// Returning the default -ENOENT as in other calls doesn't apply to mkdir
	// as the ENOENT would refer to the parent directory (which may exist).
	// Treat a mkdir inside the POSC directory as if it was an I/O error.
	if (InPoscDir(path)) {
		m_log->Log(LogMask::Debug, "POSC",
				   "Path is inside POSC directory; returning EIO", path);
		return -EIO;
	}
	return wrapPI.Mkdir(path, mode, mkpath, envP);
}

XrdOssDF *PoscFileSystem::newFile(char const *user) {
	std::unique_ptr<XrdOssDF> wrapped(m_oss->newFile(user));
	return new PoscFile(std::move(wrapped), *m_log, *m_oss, *this);
}

XrdOssDF *PoscFileSystem::newDir(char const *user) {
	std::unique_ptr<XrdOssDF> wrapped(m_oss->newDir(user));
	return new PoscDir(std::move(wrapped), *m_log, *this);
}

int PoscFileSystem::Reloc(const char *tident, const char *path,
						  const char *cgName, const char *anchor) {
	if (!path || !cgName) {
		return -ENOENT;
	}

	if (InPoscDir(path)) {
		m_log->Log(LogMask::Debug, "POSC",
				   "Failing relocation as source path is in POSC directory",
				   path);
		return -ENOENT;
	}
	if (InPoscDir(cgName)) {
		m_log->Log(LogMask::Debug, "POSC",
				   "Failing relocation as destination path in POSC directory",
				   cgName);
		return -ENOENT;
	}

	return wrapPI.Reloc(tident, path, cgName, anchor);
}

int PoscFileSystem::Remdir(const char *path, int Opts, XrdOucEnv *envP) {
	return VerifyPath(path, &XrdOss::Remdir, path, Opts, envP);
}

int PoscFileSystem::Rename(const char *oPath, const char *nPath,
						   XrdOucEnv *oEnvP, XrdOucEnv *nEnvP) {
	if (!oPath || !nPath) {
		return -ENOENT;
	}

	if (InPoscDir(oPath)) {
		m_log->Log(LogMask::Debug, "POSC",
				   "Failing rename as source path in POSC directory", oPath);
		return -ENOENT;
	}
	if (InPoscDir(oPath)) {
		m_log->Log(LogMask::Debug, "POSC",
				   "Failing rename as destination path in POSC directory",
				   nPath);
		return -ENOENT;
	}

	return wrapPI.Rename(oPath, nPath, oEnvP, nEnvP);
}

void PoscFileSystem::Shutdown() {
	std::unique_lock lock(m_shutdown_lock);
	m_shutdown_requested = true;
	m_shutdown_requested_cv.notify_one();

	m_shutdown_complete_cv.wait(lock, [] { return m_shutdown_complete; });
}

int PoscFileSystem::Stat(const char *path, struct stat *buff, int opts,
						 XrdOucEnv *env) {
	return VerifyPath(path, &XrdOss::Stat, path, buff, opts, env);
}

int PoscFileSystem::StatFS(const char *path, char *buff, int &blen,
						   XrdOucEnv *env) {
	return VerifyPath(path, &XrdOss::StatFS, path, buff, blen, env);
}

int PoscFileSystem::StatLS(XrdOucEnv &env, const char *path, char *buff,
						   int &blen) {
	return VerifyPath(path, &XrdOss::StatLS, env, path, buff, blen);
}

int PoscFileSystem::StatPF(const char *path, struct stat *buff, int opts) {
	return VerifyPath(
		path,
		static_cast<int (XrdOss::*)(const char *, struct stat *, int)>(
			&XrdOss::StatPF),
		path, buff, opts);
}

int PoscFileSystem::StatPF(const char *path, struct stat *buff) {
	return VerifyPath(path,
					  static_cast<int (XrdOss::*)(const char *, struct stat *)>(
						  &XrdOss::StatPF),
					  path, buff);
}

int PoscFileSystem::StatVS(XrdOssVSInfo *vsP, const char *sname, int updt) {
	return VerifyPath(sname, &XrdOss::StatVS, vsP, sname, updt);
}

int PoscFileSystem::StatXA(const char *path, char *buff, int &blen,
						   XrdOucEnv *env) {
	return VerifyPath(path, &XrdOss::StatXA, path, buff, blen, env);
}

int PoscFileSystem::StatXP(const char *path, unsigned long long &attr,
						   XrdOucEnv *env) {
	return VerifyPath(path, &XrdOss::StatXP, path, attr, env);
}

int PoscFileSystem::Truncate(const char *path, unsigned long long fsize,
							 XrdOucEnv *env) {
	return VerifyPath(path, &XrdOss::Truncate, path, fsize, env);
}

int PoscFileSystem::Unlink(const char *path, int Opts, XrdOucEnv *env) {
	return VerifyPath(path, &XrdOss::Unlink, path, Opts, env);
}

template <class Fn, class... Args>
int PoscFileSystem::VerifyPath(std::string_view path, Fn &&fn, Args &&...args) {
	if (InPoscDir(path)) {
		m_log->Log(LogMask::Debug, "POSC",
				   "Path is inside POSC directory; returning ENOENT",
				   path.data());
		return -ENOENT;
	}

	// Invoke the provided method `fn` on the underlying XrdOss object we are
	// wrapping (`wrapPI`). This template is agnostic to the actual arguments to
	// the method; they are just forwarded straight through.
	//
	// For example, if this object is wrapping an `S3FileSystem` object, then
	//    ```
	//    std::invoke(&XrdOss::Open, wrapPI, std::forward<Args>("/foo",
	//    O_RDONLY, 0, nullptr));
	//    ```
	// is just a funky way of saying `wrapPI->Open("/foo", O_RDONLY, 0,
	// nullptr);`
	return std::invoke(fn, wrapPI, std::forward<Args>(args)...);
}

PoscDir::~PoscDir() {}

int PoscDir::Opendir(const char *path, XrdOucEnv &env) {
	if (!path) {
		return -ENOENT;
	}
	if (m_posc_fs.InPoscDir(path)) {
		m_log.Log(LogMask::Debug, "Opendir",
				  "Ignoring directory as it is in the POSC temporary directory",
				  path);
		return -ENOENT;
	}
	m_prefix = path;
	return wrapDF.Opendir(path, env);
}

int PoscDir::Readdir(char *buff, int blen) {
	while (true) {
		auto rc = wrapDF.Readdir(buff, blen);
		if (rc) {
			if (m_stat_external) {
				memset(m_stat_external, '\0', sizeof(*m_stat_external));
			}
			return rc;
		}
		// If the auto-stat protocol is supported, then the Readdir will have
		// populated the stat buffer.  Copy it over to the user-provided one.
		// Note that we have our internal `struct stat` to prevent potential
		// bugs where ignored / invisible directory entries are leaked out
		// to the caller.
		if (m_stat_external) {
			memcpy(m_stat_external, &m_stat, sizeof(*m_stat_external));
		}
		if (*buff == '\0') {
			return 0;
		} else if (!strcmp(buff, ".") || !strcmp(buff, "..")) {
			// Always permit special current and parent directory links for
			// `Readdir`.  They allow the users of the XrdHttp web interface
			// to navigate the directory hierarchy through the rendered HTML.
			// If they're actually used to construct a path, they will get
			// normalized out by the XrdOfs layer before being passed back to
			// the XrdOss layer (this class).
			return 0;
		}
		auto path = m_prefix / std::string_view(buff, strnlen(buff, blen));
		if (m_posc_fs.InPoscDir(path)) {
			if (m_log.getMsgMask() & LogMask::Debug) {
				m_log.Log(LogMask::Debug, "Readdir",
						  "Ignoring directory component as it is in the POSC "
						  "directory",
						  path.string().c_str());
			}
			if (m_stat_external) {
				memset(m_stat_external, '\0', sizeof(*m_stat_external));
			}
		} else {
			return 0;
		}
	}
}

// Saves the provided pointer to internal memory if the
// wrapped directory supports the "auto stat" protocol.
//
int PoscDir::StatRet(struct stat *buff) {
	auto rc = wrapDF.StatRet(&m_stat);
	m_stat_external = rc ? nullptr : buff;
	return rc;
}

int PoscDir::Close(long long *retsz) {
	m_prefix.clear();
	return wrapDF.Close(retsz);
}

PoscFile::~PoscFile() {
	if (m_posc_entity) {
		if (m_posc_entity->name) {
			free(m_posc_entity->name);
		}
		if (m_posc_entity->host) {
			free(m_posc_entity->host);
		}
		if (m_posc_entity->vorg) {
			free(m_posc_entity->vorg);
		}
		if (m_posc_entity->role) {
			free(m_posc_entity->role);
		}
		if (m_posc_entity->grps) {
			free(m_posc_entity->grps);
		}
		if (m_posc_entity->creds) {
			free(m_posc_entity->creds);
		}
		if (m_posc_entity->endorsements) {
			free(m_posc_entity->endorsements);
		}
		if (m_posc_entity->moninfo) {
			free(m_posc_entity->moninfo);
		}
	}

	std::unique_lock lock(m_list_mutex);
	if (m_prev) {
		m_prev->m_next = m_next;
	}
	if (m_next) {
		m_next->m_prev = m_prev;
	}
	if (m_first == this) {
		m_first = m_next;
	}
}

void PoscFile::CopySecEntity(const XrdSecEntity &in) {
	m_posc_entity.reset(new XrdSecEntity());
	if (in.name) {
		m_posc_entity->name = strdup(in.name);
	}
	if (in.host) {
		m_posc_entity->host = strdup(in.host);
	}
	if (in.vorg) {
		m_posc_entity->vorg = strdup(in.vorg);
	}
	if (in.role) {
		m_posc_entity->role = strdup(in.role);
	}
	if (in.grps) {
		m_posc_entity->grps = strdup(in.grps);
	}
	if (in.creds && in.credslen > 0) {
		m_posc_entity->creds = strdup(in.creds);
		m_posc_entity->credslen = in.credslen;
	}
	if (in.endorsements) {
		m_posc_entity->endorsements = strdup(in.endorsements);
	}
	if (in.moninfo) {
		m_posc_entity->moninfo = strdup(in.moninfo);
	}

	if (!in.eaAPI) {
		return;
	}
	XrdSecEntityAttrCopy copyObj(*m_posc_entity->eaAPI);
	in.eaAPI->List(copyObj);
}

int PoscFile::Close(long long *retsz) {
	if (m_posc_filename.empty()) {
		return wrapDF.Close(retsz);
	}

	auto close_rv = wrapDF.Close(retsz);
	if (close_rv) {
		m_oss.Unlink(m_posc_filename.c_str(), 0, m_posc_env.get());
		m_posc_filename.clear();
		return close_rv;
	}

	auto rv =
		m_oss.Chmod(m_posc_filename.c_str(), m_posc_mode, m_posc_env.get());
	if (rv) {
		m_log.Log(LogMask::Error, "POSC", "Failed to set POSC file mode",
				  m_posc_filename.c_str(), strerror(-rv));

		m_oss.Unlink(m_posc_filename.c_str(), 0, m_posc_env.get());
		m_posc_filename.clear();
		return -EIO;
	}

	// Expected file size is advisory; if it is present, verify it matches
	// before persisting Otherwise, we don't know the expected file size, so we
	// don't verify it and just persist the file.
	if (m_expected_size > 0) {
		struct stat sb;
		rv = m_oss.Stat(m_posc_filename.c_str(), &sb, 0, m_posc_env.get());
		if (rv) {
			m_log.Log(LogMask::Error, "POSC", "Failed to stat POSC file",
					  m_posc_filename.c_str(), strerror(-rv));
			m_oss.Unlink(m_posc_filename.c_str(), 0, m_posc_env.get());
			m_posc_filename.clear();
			return -EIO;
		}
		if (sb.st_size != m_expected_size) {
			std::stringstream ss;
			m_log.Log(LogMask::Error, "POSC", ss.str().c_str(),
					  m_posc_filename.c_str());
			m_oss.Unlink(m_posc_filename.c_str(), 0, m_posc_env.get());
			m_posc_filename.clear();
			return -EIO;
		}
	}

	// At this point, we either don't know the expected file size, or the file
	// size matches the expected size. So we can persist the file.

	rv = m_oss.Rename(m_posc_filename.c_str(), m_orig_filename.c_str(),
					  m_posc_env.get(), m_posc_env.get());
	if (rv) {
		std::stringstream ss;
		ss << "Failed to rename POSC file " << m_posc_filename << " to "
		   << m_orig_filename << ": " << strerror(-rv);
		m_log.Log(LogMask::Error, "POSC", ss.str().c_str());

		m_oss.Unlink(m_posc_filename.c_str(), 0, m_posc_env.get());
		m_posc_filename.clear();
		return -EIO;
	}
	m_posc_filename.clear();
	return 0;
}

int PoscFile::Open(const char *path, int Oflag, mode_t Mode, XrdOucEnv &env) {
	if (m_posc_fs.InPoscDir(path)) {
		m_log.Log(LogMask::Debug, "POSC",
				  "Failing file open as path is in POSC directory", path);
		return -ENOENT;
	}

	if ((Oflag & (O_CREAT | O_TRUNC)) == 0) {
		return wrapDF.Open(path, Oflag, Mode, env);
	}

	// Ensure the parent directory exists; create it if needed.
	std::filesystem::path path_fs(path);
	auto parent_path = path_fs.parent_path();
	if (!parent_path.empty()) {
		struct stat sb;
		auto rv = m_oss.Stat(parent_path.c_str(), &sb, 0, &env);
		if (rv != 0) {
			if (rv == -ENOENT) {
				// Parent directory does not exist: create it (recursively)
				m_log.Log(LogMask::Debug, "POSC",
						  "Parent path does not exist; creating it",
						  parent_path.c_str());
				auto mkdir_rv =
					m_oss.Mkdir(parent_path.c_str(), 0755, /*mkpath=*/1, &env);
				if (mkdir_rv != 0) {
					m_log.Log(LogMask::Error, "POSC",
							  "Failed to create parent path",
							  parent_path.c_str(), strerror(-mkdir_rv));
					return mkdir_rv;
				}
			} else {
				m_log.Log(LogMask::Debug, "POSC",
						  "Failing file open as parent path is not accessible",
						  parent_path.c_str());
				return rv;
			}
		} else if (!S_ISDIR(sb.st_mode)) {
			m_log.Log(LogMask::Debug, "POSC",
					  "Failing file open as parent path is not a directory",
					  parent_path.c_str());
			return -ENOENT;
		}
	}

	if (env.secEnv()) {
		CopySecEntity(*env.secEnv());
	}
	int envlen;
	auto envbuff = env.Env(envlen);
	m_posc_env.reset(new XrdOucEnv(envbuff, envlen, m_posc_entity.get()));
	m_posc_mode = Mode;

	// Extract expected file size from oss.asize if available
	char *asize_char = env.Get("oss.asize");
	if (asize_char) {
		char *endptr;
		long long asize = strtoll(asize_char, &endptr, 10);
		if (endptr != asize_char && *endptr == '\0' && asize >= 0) {
			m_expected_size = static_cast<off_t>(asize);
			m_log.Log(LogMask::Debug, "POSC", "Expected file size:",
					  std::to_string(m_expected_size).c_str());
		}
	}

	m_posc_mtime.store(
		std::chrono::system_clock::now().time_since_epoch().count(),
		std::memory_order_relaxed);
	for (int idx = 0; idx < 10; ++idx) {
		m_posc_filename = m_posc_fs.GeneratePoscFile(path, env);

		auto rv = wrapDF.Open(m_posc_filename.c_str(), Oflag | O_EXCL | O_CREAT,
							  0600, env);
		if (rv >= 0) {
			m_log.Log(LogMask::Debug, "POSC", "Opened POSC file",
					  m_posc_filename.c_str());
			m_orig_filename = path;

			// Add this open file to the list of POSC files.
			std::unique_lock lock(m_list_mutex);
			if (m_first) {
				m_next = m_first;
				m_first->m_prev = this;
			}
			m_first = this;

			return rv;
		} else if (rv == -ENOENT) {
			// The per-user POSC directory does not exist; create it.
			std::filesystem::path posc_dir =
				std::filesystem::path(m_posc_filename).parent_path();
			m_log.Log(LogMask::Debug, "POSC",
					  "POSC sub-directory is needed for file creation:",
					  posc_dir.c_str());
			auto mkdir_rv = m_oss.Mkdir(posc_dir.c_str(), 0700, 1, &env);
			if (mkdir_rv != 0) {
				m_log.Log(LogMask::Error, "POSC",
						  "Failed to create POSC sub-directory",
						  posc_dir.c_str(), strerror(-mkdir_rv));
				return -EIO;
			}
		} else if (rv == -EINTR) {
			m_log.Log(LogMask::Debug, "POSC",
					  "POSC file creation interrupted; retrying",
					  m_posc_filename.c_str());
		} else if (rv != -EEXIST) {
			m_log.Log(LogMask::Error, "POSC", "Failed to open POSC file",
					  m_posc_filename.c_str(), strerror(-rv));
			// We expect the POSC file creation to always succeed; on failure,
			// we assume it's an internal error and return an EIO.
			return -EIO;
		} else {
			m_log.Log(LogMask::Debug, "POSC",
					  "Temporary POSC file already exists; trying again",
					  m_posc_filename.c_str());
		}
	}
	return -EIO;
}

ssize_t PoscFile::pgWrite(void *buffer, off_t offset, size_t wrlen,
						  uint32_t *csvec, uint64_t opts) {
	if (!m_posc_filename.empty()) {
		m_posc_mtime.store(
			std::chrono::system_clock::now().time_since_epoch().count(),
			std::memory_order_relaxed);
	}
	return wrapDF.pgWrite(buffer, offset, wrlen, csvec, opts);
}

int PoscFile::pgWrite(XrdSfsAio *aioparm, uint64_t opts) {
	if (!m_posc_filename.empty()) {
		m_posc_mtime.store(
			std::chrono::system_clock::now().time_since_epoch().count(),
			std::memory_order_relaxed);
	}
	return wrapDF.pgWrite(aioparm, opts);
}

void PoscFile::UpdateOpenFiles() {
	auto now = std::chrono::system_clock::now();
	struct timeval now_tv[2];
	gettimeofday(now_tv, nullptr);
	now_tv[1].tv_sec = now_tv[0].tv_sec;
	now_tv[1].tv_usec = now_tv[0].tv_usec;

	std::unique_lock lock(m_list_mutex);
	for (PoscFile *file = m_first; file; file = file->m_next) {
		if (file->m_posc_filename.empty()) {
			continue;
		}

		auto last_update = std::chrono::system_clock::time_point(
			std::chrono::system_clock::duration(
				file->m_posc_mtime.load(std::memory_order_relaxed)));
		if (now - last_update > m_posc_file_update) {
			file->m_posc_mtime.store(
				std::chrono::system_clock::now().time_since_epoch().count(),
				std::memory_order_relaxed);

			if (file->wrapDF.Fctl(Fctl_utimes, sizeof(now_tv),
								  reinterpret_cast<const char *>(now_tv),
								  nullptr) != 0) {
				file->m_log.Log(LogMask::Error, "POSC",
								"Failed to update POSC file mtime",
								file->m_posc_filename.c_str(), strerror(errno));
			} else {
				file->m_log.Log(LogMask::Debug, "POSC",
								"Updated POSC file mode",
								file->m_posc_filename.c_str());
			}
		}
	}
}

ssize_t PoscFile::Write(const void *buffer, off_t offset, size_t size) {
	if (!m_posc_filename.empty()) {
		m_posc_mtime.store(
			std::chrono::system_clock::now().time_since_epoch().count(),
			std::memory_order_relaxed);
	}

	return wrapDF.Write(buffer, offset, size);
}

int PoscFile::Write(XrdSfsAio *aiop) {
	if (!m_posc_filename.empty()) {
		m_posc_mtime.store(
			std::chrono::system_clock::now().time_since_epoch().count(),
			std::memory_order_relaxed);
	}

	return wrapDF.Write(aiop);
}

extern "C" {

XrdVERSIONINFO(XrdOssAddStorageSystem2, Posc);

XrdOss *XrdOssAddStorageSystem2(XrdOss *curr_oss, XrdSysLogger *logger,
								const char *config_fn, const char *parms,
								XrdOucEnv *envP) {

	std::unique_ptr<XrdSysError> log(new XrdSysError(logger, "posc_"));
	try {
		return new PoscFileSystem(curr_oss, std::move(log), config_fn, envP);
	} catch (std::runtime_error &re) {
		XrdSysError tmp_log(logger, "posc_");
		tmp_log.Emsg("Initialize",
					 "Encountered a runtime failure when initializing the "
					 "filter filesystem:",
					 re.what());
		return nullptr;
	}
}
}
