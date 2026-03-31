/***************************************************************
 *
 * Copyright (C) 2026, Pelican Project, Morgridge Institute for Research
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

#include "../src/Posc.hh"
#include "../src/shortfile.hh"

#include <XrdOss/XrdOssDefaultSS.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <XrdVersion.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <fcntl.h>
#include <sstream>
#include <string>
#include <thread>

class TestPosc : public testing::Test {
  protected:
	virtual std::string GetConfig() {
		std::stringstream ss;
		ss << "oss.localroot " << m_temp_dir << "\n";
		ss << "posc.prefix "
		   << "/posc_test\n"
			  "posc.trace debug\n";
		return ss.str();
	}

	void SetUp() override {
		setenv("XRDINSTANCE", "xrootd", 1);
		char tmp_configfn[] = "/tmp/xrootd-gtest.cfg.XXXXXX";
		int result = mkstemp(tmp_configfn);
		ASSERT_NE(result, -1) << "Failed to create temp file ("
							  << strerror(errno) << ", errno=" << errno << ")";
		m_configfn = std::string(tmp_configfn);
		close(result);

		char temp_dir[] = "/tmp/gtest_temp_xrootd_localroot.XXXXXX";
		char *res2 = mkdtemp(temp_dir);
		ASSERT_TRUE(res2) << "Failed to create temp directory ("
						  << strerror(errno) << ", errno=" << errno << ")";
		m_temp_dir = std::string(temp_dir);

		auto contents = GetConfig();
		ASSERT_FALSE(contents.empty());
		ASSERT_TRUE(writeShortFile(m_configfn, contents, 0))
			<< "Failed to write to temp file (" << strerror(errno)
			<< ", errno=" << errno << ")";

		XrdVERSIONINFODEF(XrdOssDefault, Oss, XrdVNUMBER, XrdVERSION);

		m_logger = std::make_unique<XrdSysLogger>(2, 0);

		m_default_oss = XrdOssDefaultSS(m_logger.get(), GetConfigFile().c_str(),
										XrdOssDefault);
		ASSERT_NE(m_default_oss, nullptr)
			<< "Failed to get default OSS instance";

		std::unique_ptr<XrdSysError> log(
			new XrdSysError(m_logger.get(), "posc_"));
		PoscFileSystem *posc_fs_raw;
		try {
			posc_fs_raw = new PoscFileSystem(m_default_oss, std::move(log),
											 GetConfigFile().c_str(), &m_env);
		} catch (const std::exception &e) {
			FAIL() << "Failed to create PoscFileSystem: " << e.what();
		}
		m_posc_fs.reset(posc_fs_raw);
	}

	void TearDown() override {
		m_posc_fs.reset();
		if (!m_configfn.empty()) {
			auto rv = unlink(m_configfn.c_str());
			ASSERT_EQ(rv, 0) << "Failed to delete temp file ("
							 << strerror(errno) << ", errno=" << errno << ")";
		}
		if (!m_temp_dir.empty()) {
			std::error_code ec;
			auto rv = std::filesystem::remove_all(m_temp_dir, ec);
			ASSERT_NE(rv, -1)
				<< "Failed to delete temp directory (" << ec.message()
				<< ", errno=" << ec.value() << ")";
		}
	}

	std::string GetConfigFile() const { return m_configfn; }

  protected:
	std::string m_temp_dir;
	std::string m_configfn;
	std::unique_ptr<XrdSysLogger> m_logger;
	XrdOucEnv m_env;
	XrdOss *m_default_oss{nullptr};
	std::unique_ptr<PoscFileSystem> m_posc_fs;
};

TEST_F(TestPosc, BasicFileVisibility) {
	std::unique_ptr<XrdOssDF> fp(m_posc_fs->newFile());
	ASSERT_NE(fp, nullptr) << "Failed to create new file object";

	m_env.Put("oss.asize", "0");
	auto rv = fp->Open("/testfile.txt", O_CREAT | O_RDWR, 0644, m_env);
	ASSERT_EQ(rv, 0) << "Failed to open file: " << strerror(-rv);

	rv = m_posc_fs->Stat("/testfile.txt", nullptr, 0, &m_env);
	ASSERT_NE(rv, 0) << "Temporary file is visible";
	ASSERT_EQ(rv, -ENOENT)
		<< "Stat on not-visible file should have resulted in ENOENT: "
		<< strerror(-rv);

	ASSERT_EQ(fp->Close(), 0) << "Failed to close file";

	struct stat sb;
	rv = m_posc_fs->Stat("/testfile.txt", &sb, 0, &m_env);

	ASSERT_EQ(rv, 0) << "Stat on created file failed: " << strerror(-rv);
	ASSERT_TRUE(S_ISREG(sb.st_mode))
		<< "Stat on created file did not return regular file";
	ASSERT_EQ(sb.st_size, 0) << "Stat on created file did not return size 0";

	auto write_size = strlen("Hello, POSC!");
	m_env.Put("oss.asize", std::to_string(write_size).c_str());
	rv = fp->Open("/testfile2.txt", O_CREAT | O_RDWR, 0644, m_env);
	ASSERT_EQ(rv, 0) << "Failed to open file: " << strerror(-rv);
	ASSERT_EQ(fp->Write("Hello, POSC!", 0, write_size), write_size);

	rv = m_posc_fs->Stat("/testfile2.txt", nullptr, 0, &m_env);
	ASSERT_NE(rv, 0) << "Temporary file is visible";
	ASSERT_EQ(rv, -ENOENT)
		<< "Stat on not-visible file should have resulted in ENOENT: "
		<< strerror(-rv);

	ASSERT_EQ(fp->Close(), 0) << "Failed to close file";

	memset(&sb, '\0', sizeof(sb));
	rv = m_posc_fs->Stat("/testfile2.txt", &sb, 0, &m_env);

	ASSERT_EQ(rv, 0) << "Stat on created file failed: " << strerror(-rv);
	ASSERT_TRUE(S_ISREG(sb.st_mode))
		<< "Stat on created file did not return regular file";
	ASSERT_EQ(sb.st_size, write_size)
		<< "Stat on created file did not return size " << write_size;
}

TEST_F(TestPosc, BasicFilesystemVisibility) {
	// Should not be able to stat the POSC directory
	struct stat buff;
	auto rv = m_posc_fs->Stat("/posc_test", &buff);
	ASSERT_NE(rv, 0) << "Expected stat of hidden directory to fail";
	ASSERT_EQ(rv, -ENOENT)
		<< "Expected stat of hidden directory to fail with ENOENT; got "
		<< strerror(errno);

	// Should not be able to create the POSC directory
	rv = m_posc_fs->Mkdir("/posc_test/foo", 0755, 1, &m_env);
	ASSERT_NE(rv, 0) << "Expected mkdir inside POSC dir to fail";
	ASSERT_EQ(rv, -EIO)
		<< "Expected mkdir inside POSC dir to result in an EIO.  Got "
		<< strerror(errno);

	// Listing of the parent directory should not contain the POSC directory
	auto dp_raw = m_posc_fs->newDir();
	ASSERT_NE(dp_raw, nullptr);
	std::unique_ptr<XrdOssDF> dp(dp_raw);
	rv = dp->Opendir("/", m_env);
	ASSERT_EQ(rv, 0) << "Failed to open root directory";
	char fname[NAME_MAX];
	while (((rv = dp->Readdir(fname, NAME_MAX)) == 0) && (fname[0] != '\0')) {
		fprintf(stderr, "File component: %s\n", fname);
	}
	ASSERT_EQ(rv, 0) << "Failed to list directory: " << strerror(-rv);
	ASSERT_EQ(dp->Close(), 0) << "Failed to close root directory";
}

TEST_F(TestPosc, TempfileUpdate) {
	std::unique_ptr<XrdOssDF> fp(m_posc_fs->newFile());
	ASSERT_NE(fp, nullptr) << "Failed to create new file object";

	auto rv = fp->Open("/testfile.txt", O_CREAT | O_RDWR, 0644, m_env);
	ASSERT_EQ(rv, 0) << "Failed to open file: " << strerror(-rv);

	auto pfp = dynamic_cast<PoscFile *>(fp.get());
	ASSERT_NE(fp, nullptr)
		<< "Returned file pointer should be of type PoscFile";

	auto posc_filename = pfp->GetPoscFilename();
	ASSERT_FALSE(posc_filename.empty())
		<< "POSC file is not opened in POSC mode";

	struct stat buff;
	rv = m_default_oss->Stat(posc_filename.c_str(), &buff, 0, &m_env);
	ASSERT_EQ(rv, 0) << "Failed to stat underlying POSC file";

#ifdef __APPLE__
	struct timespec now = buff.st_mtimespec;
#else
	struct timespec now = buff.st_mtim;
#endif

	auto now_chrono = std::chrono::time_point<std::chrono::system_clock,
											  std::chrono::nanoseconds>(
		std::chrono::seconds(now.tv_sec) +
		std::chrono::nanoseconds(now.tv_nsec));

	PoscFile::SetFileUpdateDuration(std::chrono::nanoseconds(100));

	std::this_thread::sleep_for(std::chrono::milliseconds(1500));

	PoscFile::UpdateOpenFiles();

	memset(&buff, '\0', sizeof(buff));
	rv = m_default_oss->Stat(posc_filename.c_str(), &buff, 0, &m_env);
	ASSERT_EQ(rv, 0) << "Failed to stat underlying POSC file";

#ifdef __APPLE__
	now = buff.st_mtimespec;
#else
	now = buff.st_mtim;
#endif

	auto update_chrono = std::chrono::time_point<std::chrono::system_clock,
												 std::chrono::nanoseconds>(
		std::chrono::seconds(now.tv_sec) +
		std::chrono::nanoseconds(now.tv_nsec));

	ASSERT_TRUE(update_chrono > now_chrono)
		<< "POSC file mtime was not updated after UpdateOpenFiles call";

	// Try expiring the user files - shouldn't do anything as the default
	// timeout is quite large.
	m_posc_fs->ExpireFiles();

	rv = m_default_oss->Stat(posc_filename.c_str(), &buff, 0, &m_env);
	ASSERT_EQ(rv, 0) << "Failed to stat underlying POSC file";

	// Decrease the file expiration time, sleep, and then expire again.
	PoscFileSystem::SetFileTimeout(std::chrono::nanoseconds(100));
	std::this_thread::sleep_for(std::chrono::milliseconds(1));

	m_posc_fs->ExpireFiles();

	rv = m_default_oss->Stat(posc_filename.c_str(), &buff, 0, &m_env);
	ASSERT_NE(rv, 0) << "Expire failed to delete underlying user file";
	ASSERT_EQ(rv, -ENOENT) << "Unexpected error when stating POSC file: "
						   << strerror(errno);
}

// Test that POSC creates parent directory on Open() if it doesn't exist
TEST_F(TestPosc, AutoCreateParentDir) {
	std::unique_ptr<XrdOssDF> fp(m_posc_fs->newFile());
	ASSERT_NE(fp, nullptr) << "Failed to create new file object";

	// Parent directory should not exist initially
	struct stat sb;
	auto rv = m_posc_fs->Stat("/subdir", &sb, 0, &m_env);
	ASSERT_EQ(rv, -ENOENT) << "Parent directory should not exist initially";

	// Open should succeed and create the parent directory
	m_env.Put("oss.asize", "0");
	rv = fp->Open("/subdir/testfile.txt", O_CREAT | O_RDWR, 0644, m_env);
	ASSERT_EQ(rv, 0) << "Open should succeed and create parent: "
					 << strerror(-rv);

	// Parent directory should exist now (created on open)
	rv = m_posc_fs->Stat("/subdir", &sb, 0, &m_env);
	ASSERT_EQ(rv, 0) << "Parent directory should exist after open: "
					 << strerror(-rv);
	ASSERT_TRUE(S_ISDIR(sb.st_mode)) << "Parent path should be a directory";

	// File should NOT exist yet (still in POSC temp location)
	rv = m_posc_fs->Stat("/subdir/testfile.txt", &sb, 0, &m_env);
	ASSERT_EQ(rv, -ENOENT) << "File should not exist before close";

	// Close should succeed and move file to final location
	ASSERT_EQ(fp->Close(), 0) << "Failed to close file";

	// Now the file should exist
	rv = m_posc_fs->Stat("/subdir/testfile.txt", &sb, 0, &m_env);
	ASSERT_EQ(rv, 0) << "File should exist after close: " << strerror(-rv);
	ASSERT_TRUE(S_ISREG(sb.st_mode)) << "Path should be a regular file";
}

// Test fixture with a bypass prefix configured
class TestPoscBypass : public TestPosc {
  protected:
	std::string GetConfig() override {
		std::stringstream ss;
		ss << "oss.localroot " << m_temp_dir << "\n";
		ss << "posc.prefix /posc_test\n"
			  "posc.trace debug\n"
			  "posc.bypass /monitoring /other/bypass\n";
		return ss.str();
	}
};

TEST_F(TestPoscBypass, BypassFileVisibleBeforeClose) {
	// Create the monitoring directory first
	auto rv = m_posc_fs->Mkdir("/monitoring", 0755, 1, &m_env);
	ASSERT_EQ(rv, 0) << "Failed to create /monitoring dir: " << strerror(-rv);

	std::unique_ptr<XrdOssDF> fp(m_posc_fs->newFile());
	ASSERT_NE(fp, nullptr) << "Failed to create new file object";

	m_env.Put("oss.asize", "0");
	rv = fp->Open("/monitoring/test.txt", O_CREAT | O_RDWR, 0644, m_env);
	ASSERT_EQ(rv, 0) << "Open under bypass prefix should succeed: "
					 << strerror(-rv);

	// Under a bypass prefix, the file should be visible BEFORE close
	// because POSC is not staging it through a temp file.
	struct stat sb;
	rv = m_posc_fs->Stat("/monitoring/test.txt", &sb, 0, &m_env);
	ASSERT_EQ(rv, 0) << "Bypassed file should be visible before close: "
					 << strerror(-rv);

	ASSERT_EQ(fp->Close(), 0) << "Failed to close file";

	rv = m_posc_fs->Stat("/monitoring/test.txt", &sb, 0, &m_env);
	ASSERT_EQ(rv, 0) << "Bypassed file should exist after close: "
					 << strerror(-rv);
}

TEST_F(TestPoscBypass, NonBypassStillUsesPosc) {
	std::unique_ptr<XrdOssDF> fp(m_posc_fs->newFile());
	ASSERT_NE(fp, nullptr) << "Failed to create new file object";

	m_env.Put("oss.asize", "0");
	auto rv = fp->Open("/normalfile.txt", O_CREAT | O_RDWR, 0644, m_env);
	ASSERT_EQ(rv, 0) << "Failed to open file: " << strerror(-rv);

	// Normal (non-bypassed) files should NOT be visible before close
	rv = m_posc_fs->Stat("/normalfile.txt", nullptr, 0, &m_env);
	ASSERT_EQ(rv, -ENOENT) << "Non-bypassed file should be hidden before close";

	ASSERT_EQ(fp->Close(), 0) << "Failed to close file";

	struct stat sb;
	rv = m_posc_fs->Stat("/normalfile.txt", &sb, 0, &m_env);
	ASSERT_EQ(rv, 0) << "File should exist after close: " << strerror(-rv);
}

TEST_F(TestPoscBypass, BypassNestedPath) {
	// Create the nested directory structure
	auto rv = m_posc_fs->Mkdir("/monitoring/selfTest", 0755, 1, &m_env);
	ASSERT_EQ(rv, 0) << "Failed to create dir: " << strerror(-rv);

	std::unique_ptr<XrdOssDF> fp(m_posc_fs->newFile());
	ASSERT_NE(fp, nullptr) << "Failed to create new file object";

	auto write_size = strlen("test content");
	m_env.Put("oss.asize", std::to_string(write_size).c_str());
	rv = fp->Open("/monitoring/selfTest/self-test-2026.txt", O_CREAT | O_RDWR,
				  0644, m_env);
	ASSERT_EQ(rv, 0) << "Open under nested bypass prefix should succeed: "
					 << strerror(-rv);

	ASSERT_EQ(fp->Write("test content", 0, write_size), write_size);

	// Should be visible before close (bypassed)
	struct stat sb;
	rv = m_posc_fs->Stat("/monitoring/selfTest/self-test-2026.txt", &sb, 0,
						 &m_env);
	ASSERT_EQ(rv, 0) << "Nested bypassed file should be visible before close: "
					 << strerror(-rv);

	ASSERT_EQ(fp->Close(), 0) << "Failed to close file";
}

TEST_F(TestPoscBypass, BypassDoesNotMatchPartialComponent) {
	std::unique_ptr<XrdOssDF> fp(m_posc_fs->newFile());
	ASSERT_NE(fp, nullptr) << "Failed to create new file object";

	// "/monitoringextra" should NOT match the bypass prefix "/monitoring"
	// (path component matching, not string prefix matching)
	auto rv = m_posc_fs->Mkdir("/monitoringextra", 0755, 1, &m_env);
	ASSERT_EQ(rv, 0) << "Failed to create dir: " << strerror(-rv);

	m_env.Put("oss.asize", "0");
	rv = fp->Open("/monitoringextra/test.txt", O_CREAT | O_RDWR, 0644, m_env);
	ASSERT_EQ(rv, 0) << "Failed to open file: " << strerror(-rv);

	// Should NOT be visible before close (not a bypass match)
	rv = m_posc_fs->Stat("/monitoringextra/test.txt", nullptr, 0, &m_env);
	ASSERT_EQ(rv, -ENOENT) << "Partial component match should not bypass POSC";

	ASSERT_EQ(fp->Close(), 0) << "Failed to close file";

	struct stat sb;
	rv = m_posc_fs->Stat("/monitoringextra/test.txt", &sb, 0, &m_env);
	ASSERT_EQ(rv, 0) << "File should exist after close: " << strerror(-rv);
}
