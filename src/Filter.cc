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

#include "Filter.hh"
#include "logging.hh"

#include <fnmatch.h>
#if defined(__GNU_SOURCE)
#define FNMATCH_FLAGS (FNM_NOESCAPE | FNM_EXTMATCH)
#else
#define FNMATCH_FLAGS (FNM_NOESCAPE)
#endif

#include <XrdOuc/XrdOucGatherConf.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSys/XrdSysError.hh>
#include <XrdVersion.hh>

#include <functional>

using namespace XrdHTTPServer;

FilterFileSystem::FilterFileSystem(XrdOss *oss, XrdSysLogger *log,
								   const char *configName, XrdOucEnv *envP)
	: XrdOssWrapper(*oss), m_oss(oss), m_log(log, "filter_") {
	if (!Config(configName)) {
		m_log.Emsg("Initialize", "Failed to configure the filter filesystem");
		throw std::runtime_error("Failed to configure the filter filesystem");
	}
	m_log.Emsg("Initialize", "FilterFileSystem initialized");
}

FilterFileSystem::~FilterFileSystem() {}

// Parse the provided file to configure the class
//
// We understand the following options:
// - filter.trace [all|error|warning|info|debug|none]
// - filter.glob [-a] [glob1] [glob2] ...
// - filter.prefix [prefix1] [prefix2]
// Each of the space-separated globs will be added to the list of permitted
// paths for the filter.  If `-a` is specified, then path components beginning
// with a `.` character will be matched.  The globs must be absolute paths.
//
// If a prefix is specified, everything underneath the prefix is permitted.
//   filter.prefix /foo
// is equivalent to
//   filter.glob -a /foo/**
bool FilterFileSystem::Config(const char *configfn) {
	m_log.setMsgMask(LogMask::Error | LogMask::Warning);

	XrdOucGatherConf filterConf("filter.trace filter.glob filter.prefix",
								&m_log);
	int result;
	if ((result = filterConf.Gather(configfn, XrdOucGatherConf::trim_lines)) <
		0) {
		m_log.Emsg("Config", -result, "parsing config file", configfn);
		return false;
	}

	char *val;
	while (filterConf.GetLine()) {
		val = filterConf.GetToken();
		if (!strcmp(val, "trace")) {
			m_log.setMsgMask(0);
			if (!(val = filterConf.GetToken())) {
				m_log.Emsg("Config",
						   "filter.trace requires an argument.  Usage: "
						   "filter.trace [all|error|warning|info|debug|none]");
				return false;
			}
			do {
				if (!strcmp(val, "all")) {
					m_log.setMsgMask(m_log.getMsgMask() | LogMask::All);
				} else if (!strcmp(val, "error")) {
					m_log.setMsgMask(m_log.getMsgMask() | LogMask::Error);
				} else if (!strcmp(val, "warning")) {
					m_log.setMsgMask(m_log.getMsgMask() | LogMask::Error |
									 LogMask::Warning);
				} else if (!strcmp(val, "info")) {
					m_log.setMsgMask(m_log.getMsgMask() | LogMask::Error |
									 LogMask::Warning | LogMask::Info);
				} else if (!strcmp(val, "debug")) {
					m_log.setMsgMask(m_log.getMsgMask() | LogMask::Error |
									 LogMask::Warning | LogMask::Info |
									 LogMask::Debug);
				} else if (!strcmp(val, "none")) {
					m_log.setMsgMask(0);
				}
			} while ((val = filterConf.GetToken()));
		} else if (!strcmp(val, "glob")) {
			if (!(val = filterConf.GetToken())) {
				m_log.Emsg("Config",
						   "filter.glob requires an argument.  "
						   "Usage: filter.glob [-a] [glob1] [glob2] ...");
				return false;
			}
			auto all = false;
			if (!strcmp(val, "-a")) {
				all = true;
				if (!(val = filterConf.GetToken())) {
					m_log.Emsg("Config",
							   "filter.glob requires an argument.  "
							   "Usage: filter.glob [-a] [glob1] [glob2] ...");
					return false;
				}
			}
			do {
				std::filesystem::path path(val);
				if (!path.is_absolute()) {
					m_log.Emsg("Config",
							   "filter.glob requires an absolute path.  Usage: "
							   "filter.glob [-a] [glob1] [glob2] ...");
					return false;
				}
				m_globs.push_back({all, std::move(path)});
			} while ((val = filterConf.GetToken()));
		} else if (!strcmp(val, "prefix")) {
			if (!(val = filterConf.GetToken())) {
				m_log.Emsg("Config",
						   "filter.prefix requires an argument.  "
						   "Usage: filter.prefix [prefix1] [prefix2] ...");
				return false;
			}
			do {
				std::filesystem::path path(val);
				if (!path.is_absolute()) {
					m_log.Emsg(
						"Config",
						"filter.prefix requires an absolute path.  Usage: "
						"filter.prefix [prefix1] [prefix2] ...");
					return false;
				}
				bool success;
				std::tie(success, path) = SanitizePrefix(path);
				if (!success) {
					m_log.Emsg("Config",
							   "filter.prefix requires an absolute prefix "
							   "without globs.  Usage: "
							   "filter.prefix [prefix1] [prefix2] ...");
					return false;
				}
				m_globs.push_back({true, (path / "**").lexically_normal()});
			} while ((val = filterConf.GetToken()));
		} else {
			m_log.Emsg("Config", "Unknown configuration directive", val);
			return false;
		}
	}
	if (m_globs.empty()) {
		m_log.Emsg("Config", "No globs specified; will allow all paths");
		return true;
	}
	for (const auto &glob : m_globs) {
		m_log.Log(LogMask::Info, "Config", "Will permit glob",
				  glob.m_glob.string().c_str(),
				  glob.m_match_dotfile ? "all" : "");
	}
	return true;
}

// Given an administrator-provided prefix, sanitize it according to our rules.
//
// The function will *fail* if one of the following is true:
// - Any path components are equal to '.' or '..'
// - Any path components contain glob special characters of '[', '*', or '?'.
//
// If the prefix is acceptable, a returned prefix will be given that is
// normalized according to std::filesystem::path's rules.
//
// Return is a boolean indicating success and the resulting prefix string.
std::pair<bool, std::string>
FilterFileSystem::SanitizePrefix(const std::filesystem::path &prefix) {
	if (!prefix.is_absolute()) {
		m_log.Emsg("SanitizePrefix", "Provided prefix must be absolute");
		return {false, ""};
	}
	for (const auto &component : prefix) {
		if (component == "." || component == "..") {
			m_log.Emsg(
				"SanitizePrefix",
				"Prefix may not contain a path component of '.' or '..':",
				prefix.c_str());
			return {false, ""};
		}
		if (component.string().find_first_of("[*?") != std::string::npos) {
			m_log.Emsg("SanitizePrefix",
					   "Prefix may not contain a path component with any of "
					   "the following characters: '*', '?', or '[':",
					   prefix.c_str());
			return {false, ""};
		}
	}
	return {true, prefix.lexically_normal()};
}

int FilterFileSystem::Chmod(const char *path, mode_t mode, XrdOucEnv *env) {
	return VerifyPath(path, true, &XrdOss::Chmod, path, mode, env);
}

int FilterFileSystem::Create(const char *tid, const char *path, mode_t mode,
							 XrdOucEnv &env, int opts) {
	return VerifyPath(path, false, &XrdOss::Create, tid, path, mode, env, opts);
}

int FilterFileSystem::Mkdir(const char *path, mode_t mode, int mkpath,
							XrdOucEnv *envP) {
	return VerifyPath(path, true, &XrdOss::Mkdir, path, mode, mkpath, envP);
}

int FilterFileSystem::Reloc(const char *tident, const char *path,
							const char *cgName, const char *anchor) {
	if (!path || !cgName) {
		return -ENOENT;
	}
	bool partial;
	if (!Glob(path, partial)) {
		m_log.Log(LogMask::Debug, "Glob",
				  "Failing relocation as source path matches no glob", path);
		return -ENOENT;
	}
	if (!Glob(cgName, partial)) {
		m_log.Log(LogMask::Debug, "Glob",
				  "Failing relocation as destination path matches no glob",
				  cgName);
		return -ENOENT;
	}
	return wrapPI.Reloc(tident, path, cgName, anchor);
}

int FilterFileSystem::Remdir(const char *path, int Opts, XrdOucEnv *envP) {
	return VerifyPath(path, true, &XrdOss::Remdir, path, Opts, envP);
}

int FilterFileSystem::Rename(const char *oPath, const char *nPath,
							 XrdOucEnv *oEnvP, XrdOucEnv *nEnvP) {
	if (!oPath || !nPath) {
		return -ENOENT;
	}
	bool partial;
	if (!Glob(oPath, partial)) {
		m_log.Log(LogMask::Debug, "Glob",
				  "Failing rename as source path matches no glob", oPath);
		return -ENOENT;
	}
	if (!Glob(nPath, partial)) {
		m_log.Log(LogMask::Debug, "Glob",
				  "Failing rename as destination path matches no glob", nPath);
		return -ENOENT;
	}
	return wrapPI.Rename(oPath, nPath, oEnvP, nEnvP);
}

int FilterFileSystem::Stat(const char *path, struct stat *buff, int opts,
						   XrdOucEnv *env) {
	return VerifyPath(path, true, &XrdOss::Stat, path, buff, opts, env);
}

int FilterFileSystem::StatFS(const char *path, char *buff, int &blen,
							 XrdOucEnv *env) {
	return VerifyPath(path, true, &XrdOss::StatFS, path, buff, blen, env);
}

int FilterFileSystem::StatLS(XrdOucEnv &env, const char *path, char *buff,
							 int &blen) {
	return VerifyPath(path, true, &XrdOss::StatLS, env, path, buff, blen);
}

int FilterFileSystem::StatPF(const char *path, struct stat *buff, int opts) {
	return VerifyPath(
		path, true,
		static_cast<int (XrdOss::*)(const char *, struct stat *, int)>(
			&XrdOss::StatPF),
		path, buff, opts);
}

int FilterFileSystem::StatPF(const char *path, struct stat *buff) {
	return VerifyPath(path, true,
					  static_cast<int (XrdOss::*)(const char *, struct stat *)>(
						  &XrdOss::StatPF),
					  path, buff);
}

int FilterFileSystem::StatVS(XrdOssVSInfo *vsP, const char *sname, int updt) {
	return VerifyPath(sname, true, &XrdOss::StatVS, vsP, sname, updt);
}

int FilterFileSystem::StatXA(const char *path, char *buff, int &blen,
							 XrdOucEnv *env) {
	return VerifyPath(path, true, &XrdOss::StatXA, path, buff, blen, env);
}

int FilterFileSystem::StatXP(const char *path, unsigned long long &attr,
							 XrdOucEnv *env) {
	return VerifyPath(path, true, &XrdOss::StatXP, path, attr, env);
}

int FilterFileSystem::Truncate(const char *path, unsigned long long fsize,
							   XrdOucEnv *env) {
	return VerifyPath(path, false, &XrdOss::Truncate, path, fsize, env);
}

int FilterFileSystem::Unlink(const char *path, int Opts, XrdOucEnv *env) {
	return VerifyPath(path, false, &XrdOss::Unlink, path, Opts, env);
}

int FilterFileSystem::Lfn2Pfn(const char *Path, char *buff, int blen) {
	return VerifyPath(Path, true,
					  static_cast<int (XrdOss::*)(const char *, char *, int)>(
						  &XrdOss::Lfn2Pfn),
					  Path, buff, blen);
}
const char *FilterFileSystem::Lfn2Pfn(const char *Path, char *buff, int blen,
									  int &rc) {
	bool partial;
	if (!Glob(Path, partial)) {
		rc = -ENOENT;
		return nullptr;
	}
	return wrapPI.Lfn2Pfn(Path, buff, blen, rc);
}

// Helper template for filesystem methods that need to verify the path passes
// the filter.
//
// If `partial_ok` is set, then a partial match is permissible (typically, this
// is done for stat- or directory-related methods to allow interacting with the
// directory hierarchy).
template <class Fn, class... Args>
int FilterFileSystem::VerifyPath(std::string_view path, bool partial_ok,
								 Fn &&fn, Args &&...args) {
	bool partial;
	if (!Glob(path, partial)) {
		m_log.Log(LogMask::Debug, "Glob", "Path matches no glob", path.data());
		return -ENOENT;
	} else if (!partial_ok && partial) {
		m_log.Log(LogMask::Debug, "Glob", "Path is a prefix of a glob",
				  path.data());
		return -EISDIR;
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

// Returns true if the path matches any of the globs, false otherwise.
//
bool FilterFileSystem::Glob(const char *path, bool &partial) {
	if (!path) {
		return false;
	}
	return Glob(std::filesystem::path(path), partial);
}

// Returns true if the path matches any of the globs, false otherwise.
//
bool FilterFileSystem::Glob(std::string_view path_view, bool &partial) {
	return Glob(std::filesystem::path(path_view), partial);
}

// Returns true if the path matches any of the globs, false otherwise.
//
// If the path is a prefix of any of the globs, `partial` will be set to true
// on return. For example, if the glob is /foo/*/*.txt and the path is
// /foo/bar, then partial will be set to true.
bool FilterFileSystem::Glob(const std::filesystem::path &path, bool &partial) {
	if (m_globs.empty()) {
		partial = false;
		return true;
	}
	if (!path.is_absolute()) {
		return false;
	}
	for (const auto &glob : m_globs) {
		if (GlobOne(path, glob, partial)) {
			return true;
		}
	}
	return false;
}

// Core logic for evaluating a path against a single glob match pattern.
//
// Returns `true` if the path matches the glob - or if the path is the prefix
// of a potential path that matches the glob.  In the latter case, `partial`
// will be set to `true` on return.
bool FilterFileSystem::GlobOne(const std::filesystem::path &path,
							   const glob &glob, bool &partial) {
	auto path_iter = path.begin();
	auto match = true;
	for (auto glob_iter = glob.m_glob.begin(); glob_iter != glob.m_glob.end();
		 ++glob_iter, ++path_iter) {
		// The path has fewer components than the provided glob.
		if (path_iter == path.end()) {
			// The globstar can match against zero components, meaning if the
			// full glob ends in globstar (and that's the next component), then
			// this is actually a full match.
			if (*glob_iter == "**" && ++glob_iter == glob.m_glob.end()) {
				partial = false;
			} else {
				partial = true;
			}
			return true;
		}
		// Logic for the "globstar" operator.  The globstar evaluates to
		// match zero-or-more paths.
		if (*glob_iter == "**") {
			auto cur_glob_component = glob_iter;
			// If the globstar is at the end of the glob, then we match
			// any subsequent part of the path.
			if (++cur_glob_component == glob.m_glob.end()) {
				partial = false;
				return true;
			} else {
				// To evaluate the globstar, we compare the remainder of the
				// glob against the remainder of the path.  Since the globstar
				// can consume any number of path components, we start with the
				// shortest possible path and recursively call `GlobOne` with
				// increasingly longer ones.
				//
				// So, if the glob is /foo/**/2*/bar and the path is
				// /foo/1/22/bar, then the new glob after the globstar will be
				// `/2/bar`.  The for-loop below will start with comparing the
				// glob `/2*/bar` against the path `/bar`, then grow the path
				// and compare against `/22/bar` (then matching).
				auto new_glob = std::filesystem::path("/");
				for (auto iter = cur_glob_component; iter != glob.m_glob.end();
					 iter++) {
					new_glob /= *iter;
				}
				// If there is a "dot file" in the path and we are not matching
				// dotfiles, then we must have a full match as the globstar
				// operator doesn't match such path components by default.
				bool has_dotfile = false;
				if (!glob.m_match_dotfile) {
					// Detect the presence of a dotfile
					for (auto iter = path_iter; iter != path.end(); iter++) {
						const auto &path_component = iter->string();
						if (!path_component.empty() &&
							path_component[0] == '.') {
							has_dotfile = true;
							break;
						}
					}
				}
				std::string cur_glob = *cur_glob_component;
				auto potential_match = true;
				for (auto back_iter = --path.end();; back_iter--) {
					auto subpath = std::filesystem::path("/");
					auto path_prefix_has_dotfile = false;
					if (has_dotfile) {
						for (auto iter = path_iter; iter != back_iter; iter++) {
							const auto &path_component = iter->string();
							if (!path_component.empty() &&
								path_component[0] == '.') {
								path_prefix_has_dotfile = true;
								break;
							}
						}
					}
					for (auto iter = back_iter; iter != path.end(); iter++) {
						subpath /= *iter;
					}
					bool subpartial;
					if (GlobOne(subpath, {glob.m_match_dotfile, new_glob},
								subpartial)) {
						if (!subpartial && !path_prefix_has_dotfile) {
							partial = false;
							return true;
						} else if (path_prefix_has_dotfile) {
							potential_match = false;
						}
					} else if (has_dotfile) {
						potential_match = false;
					}
					// By placing the break condition here, instead of in the
					// for construct, we test the case where back_iter ==
					// path_iter.
					if (back_iter == path_iter) {
						break;
					}
				}
				// The globstar can always 'consume' all the path components,
				// resuming in a partial match beyond it. That is,
				//   Path: /foo/bar/baz, Glob: /foo/**/idx.txt
				// Is going to be a partial match because the path could be the
				// prefix for
				//   /foo/bar/baz/idx.txt
				if (potential_match) {
					partial = true;
					return true;
				}
				return false;
			}
		}
		// Rely on the libc fnmatch function to implement the glob logic for a
		// single component.
		int rc;
		if (FNM_NOMATCH ==
			(rc = fnmatch(glob_iter->c_str(), path_iter->c_str(),
						  FNMATCH_FLAGS |
							  (glob.m_match_dotfile ? 0 : FNM_PERIOD)))) {
			match = false;
			break;
		} else if (rc) {
			m_log.Log(LogMask::Warning, "Glob", "Error in fnmatch for glob",
					  glob_iter->c_str(), std::to_string(rc).c_str());
		}
	}
	// If the path has more components than the glob -- and there were no
	// globstar operators found -- then we cannot have a match.  Otherwise, we
	// consumed all the glob and path components and we have a full match.
	if (path_iter != path.end()) {
		match = false;
	}
	if (match) {
		partial = false;
		return true;
	}
	return false;
}

XrdOssDF *FilterFileSystem::newFile(char const *user) {
	std::unique_ptr<XrdOssDF> wrapped(m_oss->newFile(user));
	return new FilterFile(std::move(wrapped), m_log, *this);
}

XrdOssDF *FilterFileSystem::newDir(char const *user) {
	std::unique_ptr<XrdOssDF> wrapped(m_oss->newDir(user));
	return new FilterDir(std::move(wrapped), m_log, *this);
}

FilterDir::~FilterDir() {}

int FilterDir::Opendir(const char *path, XrdOucEnv &env) {
	if (!path) {
		return -ENOENT;
	}
	bool partial;
	if (!m_oss.Glob(path, partial)) {
		m_log.Log(LogMask::Debug, "Opendir",
				  "Ignoring directory as it passes no glob", path);
		return -ENOENT;
	}
	m_prefix = path;
	return wrapDF.Opendir(path, env);
}

int FilterDir::Readdir(char *buff, int blen) {
	m_stat_avail = false;
	while (true) {
		auto rc = wrapDF.Readdir(buff, blen);
		if (rc) {
			return rc;
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
		bool partial;
		if (m_oss.Glob(path, partial)) {
			if (partial) {
				struct stat buff;
				auto rc = StatRet(&buff);
				if (rc) {
					return rc;
				}
				if (buff.st_mode & S_IFDIR) {
					return 0;
				}
				m_stat_avail = false;
				if (m_log.getMsgMask() & LogMask::Debug) {
					m_log.Log(LogMask::Debug, "Readdir",
							  "Ignoring file in directory as it is a prefix "
							  "for a glob",
							  path.string().c_str());
				}
			} else {
				return 0;
			}
		} else if (m_log.getMsgMask() & LogMask::Debug) {
			m_log.Log(LogMask::Debug, "Readdir",
					  "Ignoring directory component as it passes no glob",
					  path.string().c_str());
		}
	}
}

// Returns the struct stat corresponding to the current
// directory entry name.
//
// If `Readdir` required a stat of the path to determine
// if its visible, the cached copy may be served here.
int FilterDir::StatRet(struct stat *buff) {
	if (m_stat_avail) {
		memcpy(buff, &m_stat, sizeof(m_stat));
		return 0;
	}
	auto rc = wrapDF.StatRet(&m_stat);
	if (!rc) {
		m_stat_avail = true;
		memcpy(buff, &m_stat, sizeof(m_stat));
	}
	return rc;
}

int FilterDir::Close(long long *retsz) {
	m_prefix.clear();
	return wrapDF.Close(retsz);
}

FilterFile::~FilterFile() {}

int FilterFile::Open(const char *path, int Oflag, mode_t Mode, XrdOucEnv &env) {
	bool partial;
	if (!m_oss.Glob(path, partial)) {
		m_log.Log(LogMask::Debug, "Glob",
				  "Failing file open as path matches no glob", path);
		return -ENOENT;
	} else if (partial) {
		m_log.Log(LogMask::Debug, "Glob",
				  "Failing file open as path is a prefix of a glob", path);
		return -EISDIR;
	}
	return wrapDF.Open(path, Oflag, Mode, env);
}

extern "C" {

XrdVERSIONINFO(XrdOssAddStorageSystem2, Filter);

XrdOss *XrdOssAddStorageSystem2(XrdOss *curr_oss, XrdSysLogger *logger,
								const char *config_fn, const char *parms,
								XrdOucEnv *envP) {

	XrdSysError log(logger, "filter_");
	try {
		return new FilterFileSystem(curr_oss, logger, config_fn, envP);
	} catch (std::runtime_error &re) {
		log.Emsg("Initialize",
				 "Encountered a runtime failure when initializing the "
				 "filter filesystem:",
				 re.what());
		return nullptr;
	}
}
}
