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

#include "../src/Filter.hh"
#include "s3_tests_common.hh"

#include <XrdOss/XrdOss.hh>
#include <XrdOuc/XrdOucEnv.hh>

#include <string>

class SimpleDir final : public XrdOssDF {
  public:
	virtual int Opendir(const char *path, XrdOucEnv &env) {
		if (!strncmp(path, "/prefix", 7)) {
			m_subdir = !strcmp(path, "/prefix3");
			return 0;
		}
		return -ENOENT;
	}
	virtual int Readdir(char *buff, int blen) {
		if (m_idx >= 3) {
			if (m_subdir && m_idx == 3 && blen >= 8) {
				memcpy(buff, "idx.txt", 8);
				m_idx++;
				return 0;
			}
			buff[0] = '\0';
			return 0;
		}
		auto result = (m_subdir ? "subdir" : "idx") + std::to_string(m_idx++) +
					  (m_subdir ? "" : ".txt");
		if (result.size() + 1 < static_cast<unsigned>(blen)) {
			memcpy(buff, result.c_str(), result.size());
			buff[result.size()] = '\0';
		} else {
			return -ENOMEM;
		}
		return 0;
	}
	virtual int StatRet(struct stat *buff) {
		if (!buff)
			return 0;
		memset(buff, '\0', sizeof(struct stat));
		buff->st_mode = 0750 | ((m_subdir && m_idx <= 3) ? S_IFDIR : S_IFREG);
		buff->st_size = m_idx;
		return 0;
	}
	virtual int Close(long long *retsz = 0) {
		m_idx = 0;
		return 0;
	}

  private:
	bool m_subdir{false};
	unsigned m_idx{0};
};

class SimpleFile final : public XrdOssDF {
  public:
	virtual int Fchmod(mode_t mode) { return 0; }
	virtual void Flush() {}
	virtual int Fstat(struct stat *buf) {
		if (!buf)
			return 0;
		memset(buf, '\0', sizeof(struct stat));
		buf->st_mode = 0640 | S_IFREG;
		return 0;
	}
	virtual int Fsync() { return 0; }
	virtual int Fsync(XrdSfsAio *aiop) { return 0; }
	virtual int Ftruncate(unsigned long long flen) { return 0; }
	virtual int Open(const char *path, int Oflag, mode_t Mode, XrdOucEnv &env) {
		return 0;
	}
	virtual ssize_t pgRead(void *buffer, off_t offset, size_t rdlen,
						   uint32_t *csvec, uint64_t opts) {
		return 0;
	}
	virtual int pgRead(XrdSfsAio *aioparm, uint64_t opts) { return 0; }
	virtual ssize_t pgWrite(void *buffer, off_t offset, size_t wrlen,
							uint32_t *csvec, uint64_t opts) {
		return 0;
	}
	virtual int pgWrite(XrdSfsAio *aioparm, uint64_t opts) { return 0; }
	virtual ssize_t Read(off_t offset, size_t size) { return 0; }
	virtual ssize_t Read(void *buffer, off_t offset, size_t size) { return 0; }
	virtual int Read(XrdSfsAio *aiop) {
		(void)aiop;
		return 0;
	}
	virtual ssize_t ReadRaw(void *buffer, off_t offset, size_t size) {
		return 0;
	}
	virtual ssize_t ReadV(XrdOucIOVec *readV, int rdvcnt) { return 0; }
	virtual ssize_t Write(const void *buffer, off_t offset, size_t size) {
		return 0;
	}
	virtual int Write(XrdSfsAio *aiop) {
		(void)aiop;
		return 0;
	}
	virtual ssize_t WriteV(XrdOucIOVec *writeV, int wrvcnt) { return 0; }
	virtual int Close(long long *retsz = 0) { return 0; }
	virtual int Fctl(int cmd, int alen, const char *args, char **resp = 0) {
		return 0;
	}
};

class SimpleFilesystem final : public XrdOss {
  public:
	virtual XrdOssDF *newDir(char const *user) { return new SimpleDir; }
	virtual XrdOssDF *newFile(char const *user) { return new SimpleFile; }
	virtual int Chmod(const char *path, mode_t mode, XrdOucEnv *envP = 0) {
		return 0;
	}
	virtual int Create(const char *tid, const char *path, mode_t mode,
					   XrdOucEnv &env, int opts = 0) {
		return 0;
	}
	virtual int Init(XrdSysLogger *lp, const char *cfn) { return 0; }
	virtual int Mkdir(const char *path, mode_t mode, int mkpath = 0,
					  XrdOucEnv *envP = 0) {
		return 0;
	}
	virtual int Remdir(const char *path, int Opts = 0, XrdOucEnv *envP = 0) {
		return 0;
	}
	virtual int Rename(const char *oPath, const char *nPath,
					   XrdOucEnv *oEnvP = 0, XrdOucEnv *nEnvP = 0) {
		return 0;
	}
	virtual int Stat(const char *path, struct stat *buff, int opts = 0,
					 XrdOucEnv *envP = 0);
	virtual int Truncate(const char *path, unsigned long long fsize,
						 XrdOucEnv *envP = 0) {
		return 0;
	}
	virtual int Unlink(const char *path, int Opts = 0, XrdOucEnv *envP = 0) {
		return 0;
	}
	virtual ~SimpleFilesystem() {}
};

int SimpleFilesystem::Stat(const char *path, struct stat *buff, int opts,
						   XrdOucEnv *envP) {
	if (!strcmp(path, "/prefix1") || !strcmp(path, "/prefix2") ||
		!strcmp(path, "/prefix3") || !strcmp(path, "/prefix4") ||
		!strcmp(path, "/prefix3/subdir1") ||
		!strcmp(path, "/prefix3/subdir2") ||
		!strcmp(path, "/prefix3/subdir3") ||
		!strcmp(path, "/prefix3/subdir4")) {
		if (!buff)
			return 0;
		memset(buff, '\0', sizeof(struct stat));
		buff->st_mode = 0750 | S_IFDIR;
		return 0;
	}
	if (!strcmp(path, "/prefix1/idx0.txt") ||
		!strcmp(path, "/prefix2/idx1.txt") ||
		!strcmp(path, "/prefix2/idx2.txt") ||
		!strcmp(path, "/prefix2/idx3.txt") ||
		!strcmp(path, "/prefix2/idx4.txt") ||
		!strcmp(path, "/prefix3/subdir1/1.txt") ||
		!strcmp(path, "/prefix3/subdir1/2.txt") ||
		!strcmp(path, "/prefix3/subdir1/3.txt") ||
		!strcmp(path, "/prefix3/subdir1/4.txt") ||
		!strcmp(path, "/prefix3/subdir2/1.txt") ||
		!strcmp(path, "/prefix3/subdir2/2.txt") ||
		!strcmp(path, "/prefix3/subdir2/3.txt") ||
		!strcmp(path, "/prefix3/subdir1/4.txt") ||
		!strcmp(path, "/prefix3/subdir3/1.txt") ||
		!strcmp(path, "/prefix3/subdir3/2.txt") ||
		!strcmp(path, "/prefix3/subdir3/3.txt") ||
		!strcmp(path, "/prefix3/subdir3/4.txt") ||
		!strcmp(path, "/prefix3/subdir4/1.txt") ||
		!strcmp(path, "/prefix4/subdir2/idx0.txt") ||
		!strcmp(path, "/prefix5/idx.txt")) {
		if (!buff)
			return 0;
		memset(buff, '\0', sizeof(struct stat));
		buff->st_mode = 0750 | S_IFREG;
		return 0;
	}
	return -ENOENT;
}

class FileSystemGlob : public FileSystemFixtureBase {
  protected:
	virtual std::string GetConfig() override {
		return R"(
filter.glob /prefix1 /prefix2/*.txt
filter.glob /prefix3/*/*.txt
filter.prefix /prefix5
filter.trace all
)";
	}
};

TEST_F(FileSystemGlob, GlobFilter) {
	SimpleFilesystem sfs;
	XrdSysLogger log;
	FilterFileSystem fs(new SimpleFilesystem, &log, m_configfn.c_str(),
						nullptr);
	XrdOucEnv env;

	struct stat buf;
	ASSERT_EQ(sfs.Stat("/prefix1", &buf), 0);
	ASSERT_EQ(fs.Stat("/prefix1", &buf), 0);
	ASSERT_EQ(sfs.Stat("/prefix1/idx0.txt", &buf), 0);
	ASSERT_EQ(fs.Stat("/prefix1/idx0.txt", &buf), -ENOENT);
	ASSERT_EQ(fs.Stat("/prefix5/idx0.txt", &buf), -ENOENT);
	ASSERT_EQ(fs.Stat("/prefix5/idx.txt", &buf), 0);

	std::unique_ptr<XrdOssDF> sfsdir(sfs.newDir(""));
	ASSERT_NE(nullptr, sfsdir);
	ASSERT_EQ(sfsdir->Opendir("/prefix1", env), 0);
	char buff[256];
	ASSERT_EQ(sfsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "idx0.txt");
	ASSERT_EQ(sfsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "idx1.txt");
	ASSERT_EQ(sfsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "idx2.txt");
	ASSERT_EQ(sfsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "");
	ASSERT_EQ(sfsdir->Close(), 0);

	std::unique_ptr<XrdOssDF> fsdir(fs.newDir());
	ASSERT_NE(nullptr, fsdir);
	ASSERT_EQ(fsdir->Opendir("/prefix1", env), 0);
	ASSERT_EQ(fsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "");
	ASSERT_EQ(fsdir->Close(), 0);

	ASSERT_EQ(fsdir->Opendir("/prefix2", env), 0);
	ASSERT_EQ(fsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "idx0.txt");
	ASSERT_EQ(fsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "idx1.txt");
	ASSERT_EQ(fsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "idx2.txt");
	ASSERT_EQ(fsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "");
	ASSERT_EQ(fsdir->Close(), 0);

	sfsdir.reset(sfs.newDir(""));
	ASSERT_NE(sfsdir.get(), nullptr);
	ASSERT_EQ(sfsdir->Opendir("/prefix3", env), 0);
	ASSERT_EQ(sfsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "subdir0");
	ASSERT_EQ(sfsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "subdir1");
	ASSERT_EQ(sfsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "subdir2");
	ASSERT_EQ(sfsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "idx.txt");
	ASSERT_EQ(sfsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "");
	ASSERT_EQ(sfsdir->Close(), 0);

	ASSERT_EQ(fsdir->Opendir("/prefix3", env), 0);
	ASSERT_EQ(fsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "subdir0");
	ASSERT_EQ(fsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "subdir1");
	ASSERT_EQ(fsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "subdir2");
	ASSERT_EQ(fsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "");
	ASSERT_EQ(fsdir->Close(), 0);
	ASSERT_EQ(fsdir->Opendir("/prefix3/subdir0", env), 0);
	ASSERT_EQ(fsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "idx0.txt");
	ASSERT_EQ(fsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "idx1.txt");
	ASSERT_EQ(fsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "idx2.txt");
	ASSERT_EQ(fsdir->Readdir(buff, 256), 0);
	ASSERT_STREQ(buff, "");
	ASSERT_EQ(fsdir->Close(), 0);

	std::unique_ptr<XrdOssDF> fsfile(fs.newFile());
	ASSERT_NE(nullptr, fsfile);
	ASSERT_EQ(fsfile->Open("/prefix1/idx0.txt", 0, 0, env), -ENOENT);
	std::unique_ptr<XrdOssDF> sfsfile(sfs.newFile(""));
	ASSERT_NE(nullptr, sfsfile);
	ASSERT_EQ(sfsfile->Open("/prefix1/idx0.txt", 0, 0, env), 0);
	fsfile.reset(fs.newFile());
	ASSERT_NE(nullptr, fsfile);
	ASSERT_EQ(fsfile->Open("/prefix2/idx0.txt", 0, 0, env), 0);
	fsfile.reset(fs.newFile());
	ASSERT_NE(nullptr, fsfile);
	ASSERT_EQ(fsfile->Open("/prefix3/subdir2/idx0.txt", 0, 0, env), 0);
	fsfile.reset(fs.newFile());
	ASSERT_NE(nullptr, fsfile);
	ASSERT_EQ(fsfile->Open("/prefix4/subdir2/idx0.txt", 0, 0, env), -ENOENT);
	sfsfile.reset(sfs.newFile(""));
	ASSERT_NE(nullptr, sfsfile);
	ASSERT_EQ(sfsfile->Open("/prefix4/subdir2/idx0.txt", 0, 0, env), 0);
}

TEST_F(FileSystemGlob, GlobNormal) {
	XrdSysLogger log;
	FilterFileSystem fs(new SimpleFilesystem, &log, m_configfn.c_str(),
						nullptr);
	XrdOucEnv env;
	bool partial;
	XrdSysError dst(&log, "FileSystemGlob");

	dst.Emsg("Glob", "Testing /");
	ASSERT_TRUE(fs.GlobOne("/", {false, "/*"}, partial));
	ASSERT_TRUE(partial);
	ASSERT_TRUE(fs.GlobOne("/", {false, "/"}, partial));
	ASSERT_FALSE(partial);

	dst.Emsg("Glob", "Testing /foo");
	ASSERT_TRUE(fs.GlobOne("/foo", {false, "/*"}, partial));
	ASSERT_FALSE(partial);
	dst.Emsg("Globstar", "Testing /bar");
	ASSERT_FALSE(fs.GlobOne("/foo", {false, "/bar"}, partial));
	ASSERT_FALSE(partial);

	dst.Emsg("Glob", "Testing /foo/bar/idx.txt");
	ASSERT_FALSE(fs.GlobOne("/foo/bar/idx.txt", {false, "/foo/*"}, partial));
	ASSERT_FALSE(partial);
	ASSERT_TRUE(
		fs.GlobOne("/foo/bar/idx.txt", {false, "/foo/bar/idx.txt"}, partial));
	ASSERT_FALSE(partial);
	ASSERT_TRUE(fs.GlobOne("/foo/bar/idx.txt", {false, "/foo/bar/idx.txt/baz"},
						   partial));
	ASSERT_TRUE(partial);
	ASSERT_TRUE(
		fs.GlobOne("/foo/bar/idx.txt", {false, "/foo/*/idx.txt"}, partial));
	ASSERT_FALSE(partial);
	ASSERT_TRUE(
		fs.GlobOne("/foo/bar/idx.txt", {false, "/foo/*/*.txt"}, partial));
	ASSERT_FALSE(partial);
	ASSERT_TRUE(
		fs.GlobOne("/foo/bar/idx.txt", {false, "/foo/bar/*.txt"}, partial));
	ASSERT_FALSE(partial);
	ASSERT_TRUE(
		fs.GlobOne("/foo/bar/idx.txt", {false, "/foo/bar/idx.*"}, partial));
	ASSERT_FALSE(partial);
	ASSERT_FALSE(
		fs.GlobOne("/foo/bar/idx.txt", {false, "/foo/bar/t.*"}, partial));
	ASSERT_FALSE(partial);

	dst.Emsg("Glob", "Testing /foo/.bar/idx.txt");
	ASSERT_TRUE(
		fs.GlobOne("/foo/.bar/idx.txt", {true, "/foo/*/idx.txt"}, partial));
	ASSERT_FALSE(partial);
	ASSERT_FALSE(
		fs.GlobOne("/foo/.bar/idx.txt", {false, "/foo/*/idx.txt"}, partial));
	dst.Emsg("Glob", "Testing /.bar");
	ASSERT_TRUE(fs.GlobOne("/.bar", {true, "/*"}, partial));
	ASSERT_FALSE(partial);
	ASSERT_FALSE(fs.GlobOne("/.bar", {false, "/*"}, partial));
}

TEST_F(FileSystemGlob, Globstar) {
	XrdSysLogger log;
	FilterFileSystem fs(new SimpleFilesystem, &log, m_configfn.c_str(),
						nullptr);
	XrdOucEnv env;
	bool partial;
	XrdSysError dst(&log, "FileSystemGlob");
	dst.Emsg("Globstar", "Testing /some/path");
	ASSERT_TRUE(fs.GlobOne("/some/path", {false, "/some/**"}, partial));
	ASSERT_FALSE(partial);
	dst.Emsg("Globstar", "Testing /");
	ASSERT_TRUE(fs.GlobOne("/", {false, "/**"}, partial));
	ASSERT_FALSE(partial);
	dst.Emsg("Globstar", "Testing /some");
	ASSERT_TRUE(fs.GlobOne("/some", {false, "/**"}, partial));
	ASSERT_FALSE(partial);
	dst.Emsg("Globstar", "Testing /some");
	ASSERT_TRUE(fs.GlobOne("/some", {false, "/some/**"}, partial));
	ASSERT_FALSE(partial);
	dst.Emsg("Globstar", "Testing /some/path/subdir/foo.txt");
	ASSERT_TRUE(
		fs.GlobOne("/some/path/subdir/foo.txt", {false, "/some/**"}, partial));
	ASSERT_FALSE(partial);
	dst.Emsg("Globstar", "Testing /foo/bar/idx.txt");
	ASSERT_TRUE(
		fs.GlobOne("/foo/bar/idx.txt", {false, "/foo/**/idx.txt"}, partial));
	ASSERT_FALSE(partial);
	dst.Emsg("Globstar", "Testing /foo/bar/baz/idx.txt");
	ASSERT_TRUE(fs.GlobOne("/foo/bar/baz/idx.txt", {false, "/foo/**/idx.txt"},
						   partial));
	ASSERT_FALSE(partial);
	dst.Emsg("Globstar", "Testing /foo/idx.txt");
	ASSERT_TRUE(
		fs.GlobOne("/foo/idx.txt", {false, "/foo/**/idx.txt"}, partial));
	ASSERT_FALSE(partial);
	dst.Emsg("Globstar", "Testing /foo/bar/idx.txt");
	ASSERT_TRUE(fs.GlobOne("/foo/bar/idx.txt", {false, "/foo/**/bar/idx.txt"},
						   partial));
	ASSERT_FALSE(partial);
	dst.Emsg("Globstar", "Testing /foo/bar/bar/idx.txt");
	ASSERT_TRUE(fs.GlobOne("/foo/bar/bar/idx.txt",
						   {false, "/foo/**/bar/idx.txt"}, partial));
	ASSERT_FALSE(partial);
	dst.Emsg("Globstar", "Testing /foo/bar/bar");
	ASSERT_TRUE(
		fs.GlobOne("/foo/bar/bar", {false, "/foo/**/bar/idx.txt"}, partial));
	ASSERT_TRUE(partial);
	dst.Emsg("Globstar", "Testing /foo/bar/idx.txt");
	ASSERT_TRUE(
		fs.GlobOne("/foo/bar/idx.txt", {false, "/foo/**/false"}, partial));
	ASSERT_TRUE(partial);

	// Test that "dot files" are not matched by the globstar operator,
	// matching the bash implementation.
	dst.Emsg("Globstar", "Testing /foo/.bar/idx.txt");
	partial = false;
	ASSERT_FALSE(
		fs.GlobOne("/foo/.bar/idx.txt", {false, "/foo/**/idx.txt"}, partial));
	ASSERT_FALSE(partial);
	ASSERT_TRUE(
		fs.GlobOne("/foo/.bar/idx.txt", {true, "/foo/**/idx.txt"}, partial));
	ASSERT_FALSE(partial);
	ASSERT_TRUE(
		fs.GlobOne("/foo/.bar/idx.txt", {true, "/foo/**/bar.txt"}, partial));
	ASSERT_TRUE(partial);
	partial = false;
	dst.Emsg("Globstar", "Testing negative match with dotfile");
	ASSERT_FALSE(
		fs.GlobOne("/foo/.bar/idx.txt", {false, "/foo/**/bar.txt"}, partial));
	ASSERT_FALSE(partial);
	ASSERT_TRUE(
		fs.GlobOne("/foo/.bar/idx.txt", {true, "/foo/**/bar.txt"}, partial));
	ASSERT_TRUE(partial);
	dst.Emsg("Globstra", "Testing /foo/1/.bar/idx.txt");
	ASSERT_FALSE(
		fs.GlobOne("/foo/1/.bar/idx.txt", {false, "/foo/**/idx.txt"}, partial));
	ASSERT_TRUE(fs.GlobOne("/foo/1/.bar/idx.txt",
						   {false, "/foo/**/.bar/idx.txt"}, partial));
	ASSERT_TRUE(fs.GlobOne("/foo/1/.bar/idx.txt",
						   {false, "/foo/**/1/.bar/idx.txt"}, partial));
	dst.Emsg("Globstra", "Testing /foo/.1/.bar/idx.txt");
	ASSERT_FALSE(fs.GlobOne("/foo/.1/.bar/idx.txt",
							{false, "/foo/**/.bar/idx.txt"}, partial));
}
