/***************************************************************
 *
 * Copyright (C) 2024, Pelican Project, Morgridge Institute for Research
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

#include "../src/S3Commands.hh"
#include "../src/S3FileSystem.hh"
#include "../src/shortfile.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdSys/XrdSysError.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <gtest/gtest.h>

class TestAmazonRequest : public AmazonRequest {
  public:
	XrdSysLogger log{};
	XrdSysError err{&log, "TestS3CommandsLog"};

	TestAmazonRequest(const std::string &url, const std::string &akf,
					  const std::string &skf, const std::string &bucket,
					  const std::string &object, const std::string &path,
					  int sigVersion)
		: AmazonRequest(url, akf, skf, bucket, object, path, sigVersion, err) {}

	// For getting access to otherwise-protected members
	std::string getHostUrl() const { return hostUrl; }
};

TEST(TestS3URLGeneration, Test1) {
	const std::string serviceUrl = "https://s3-service.com:443";
	const std::string b = "test-bucket";
	const std::string o = "test-object";

	// Test path-style URL generation
	TestAmazonRequest pathReq{serviceUrl, "akf", "skf", b, o, "path", 4};
	std::string generatedHostUrl = pathReq.getHostUrl();
	ASSERT_EQ(generatedHostUrl,
			  "https://s3-service.com:443/test-bucket/test-object");

	// Test virtual-style URL generation
	TestAmazonRequest virtReq{serviceUrl, "akf", "skf", b, o, "virtual", 4};
	generatedHostUrl = virtReq.getHostUrl();
	ASSERT_EQ(generatedHostUrl,
			  "https://test-bucket.s3-service.com:443/test-object");

	// Test path-style with empty bucket (which we use for exporting an entire
	// endpoint)
	TestAmazonRequest pathReqNoBucket{serviceUrl, "akf",  "skf", "",
									  o,		  "path", 4};
	generatedHostUrl = pathReqNoBucket.getHostUrl();
	ASSERT_EQ(generatedHostUrl, "https://s3-service.com:443/test-object");
}

class FileSystemFixtureBase : public testing::Test {
  protected:
	FileSystemFixtureBase()
		: m_log(new XrdSysLogger(2, 0)) // Log to stderr, no log rotation
	{}

	void SetUp() override {
		setenv("XRDINSTANCE", "xrootd", 1);
		char tmp_configfn[] = "/tmp/xrootd-s3-gtest.cfg.XXXXXX";
		auto result = mkstemp(tmp_configfn);
		ASSERT_NE(result, -1) << "Failed to create temp file ("
							  << strerror(errno) << ", errno=" << errno << ")";
		m_configfn = std::string(tmp_configfn);

		auto contents = GetConfig();
		ASSERT_FALSE(contents.empty());
		ASSERT_TRUE(writeShortFile(m_configfn, contents, 0))
			<< "Failed to write to temp file (" << strerror(errno)
			<< ", errno=" << errno << ")";
	}

	void TearDown() override {
		if (!m_configfn.empty()) {
			auto rv = unlink(m_configfn.c_str());
			ASSERT_EQ(rv, 0) << "Failed to delete temp file ("
							 << strerror(errno) << ", errno=" << errno << ")";
		}
	}

	virtual std::string GetConfig() = 0;

	std::string m_configfn;
	std::unique_ptr<XrdSysLogger> m_log;
};

class FileSystemS3VirtualBucket : public FileSystemFixtureBase {
  protected:
	virtual std::string GetConfig() override {
		return R"(
s3.begin
s3.path_name        /test
s3.bucket_name      genome-browser
s3.service_name     s3.amazonaws.com
s3.region           us-east-1
s3.service_url      https://s3.us-east-1.amazonaws.com
s3.url_style        virtual
s3.end
)";
	}
};

class FileSystemS3VirtualNoBucket : public FileSystemFixtureBase {
  protected:
	virtual std::string GetConfig() override {
		return R"(
s3.begin
s3.path_name        /test
s3.service_name     s3.amazonaws.com
s3.region           us-east-1
s3.service_url      https://s3.us-east-1.amazonaws.com
s3.url_style        virtual
s3.end
)";
	}
};

class FileSystemS3PathBucket : public FileSystemFixtureBase {
  protected:
	virtual std::string GetConfig() override {
		return R"(
s3.begin
s3.path_name        /test
s3.service_name     s3.amazonaws.com
s3.region           us-east-1
s3.bucket_name      genome-browser
s3.service_url      https://s3.us-east-1.amazonaws.com
s3.url_style        path
s3.end
)";
	}
};

class FileSystemS3PathNoBucket : public FileSystemFixtureBase {
  protected:
	virtual std::string GetConfig() override {
		return R"(
s3.begin
s3.path_name        /test
s3.service_name     s3.amazonaws.com
s3.region           us-east-1
s3.service_url      https://s3.us-east-1.amazonaws.com
s3.url_style        path
s3.end
)";
	}
};

// Regression test for when the service_url ends in a `/`
class FileSystemS3PathBucketSlash : public FileSystemFixtureBase {
  protected:
	virtual std::string GetConfig() override {
		return R"(
s3.begin
s3.path_name        /test
s3.service_name     s3.amazonaws.com
s3.region           us-east-1
s3.bucket_name      genome-browser
s3.service_url      https://s3.us-east-1.amazonaws.com/
s3.url_style        path
s3.end
)";
	}
};

void TestDirectoryContents(S3FileSystem &fs, const std::string &dirname) {
	std::unique_ptr<XrdOssDF> dir(fs.newDir());
	ASSERT_TRUE(dir);

	XrdOucEnv env;
	auto rv = dir->Opendir(dirname.c_str(), env);
	ASSERT_EQ(rv, 0);

	struct stat buf;
	ASSERT_EQ(dir->StatRet(&buf), 0);

	std::vector<char> name;
	name.resize(255);

	rv = dir->Readdir(&name[0], 255);
	ASSERT_EQ(rv, 0);
	ASSERT_EQ(std::string(&name[0]), "cellbrowser.json.bak");
	ASSERT_EQ(buf.st_mode & S_IFREG,
			  static_cast<decltype(buf.st_mode)>(S_IFREG));
	ASSERT_EQ(buf.st_size, 672);

	rv = dir->Readdir(&name[0], 255);
	ASSERT_EQ(rv, 0);
	ASSERT_EQ(std::string(&name[0]), "dataset.json");
	ASSERT_EQ(buf.st_mode & S_IFREG,
			  static_cast<decltype(buf.st_mode)>(S_IFREG));
	ASSERT_EQ(buf.st_size, 1847);

	rv = dir->Readdir(&name[0], 255);
	ASSERT_EQ(rv, 0);
	ASSERT_EQ(std::string(&name[0]), "desc.json");
	ASSERT_EQ(buf.st_mode & S_IFREG,
			  static_cast<decltype(buf.st_mode)>(S_IFREG));
	ASSERT_EQ(buf.st_size, 1091);

	rv = dir->Readdir(&name[0], 255);
	ASSERT_EQ(rv, 0);
	ASSERT_EQ(std::string(&name[0]), "all");
	ASSERT_EQ(buf.st_mode & S_IFDIR,
			  static_cast<decltype(buf.st_mode)>(S_IFDIR));

	rv = dir->Readdir(&name[0], 255);
	ASSERT_EQ(rv, 0);
	ASSERT_EQ(std::string(&name[0]), "by-organ");
	ASSERT_EQ(buf.st_mode & S_IFDIR,
			  static_cast<decltype(buf.st_mode)>(S_IFDIR));

	rv = dir->Readdir(&name[0], 255);
	ASSERT_EQ(rv, 0);
	ASSERT_EQ(std::string(&name[0]), "func-compart");
	ASSERT_EQ(buf.st_mode & S_IFDIR,
			  static_cast<decltype(buf.st_mode)>(S_IFDIR));

	rv = dir->Readdir(&name[0], 255);
	ASSERT_EQ(rv, 0);
	ASSERT_EQ(std::string(&name[0]), "");

	ASSERT_EQ(dir->Close(), 0);
}

TEST_F(FileSystemS3VirtualBucket, Create) {
	EXPECT_NO_THROW(
		{ S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr); });
}

TEST_F(FileSystemS3VirtualBucket, Stat) {
	S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr);
	struct stat buff;
	auto rv = fs.Stat("/test/cells/tabula-sapiens/cellbrowser.json.bak", &buff);
	ASSERT_EQ(rv, 0) << "Failed to stat AWS bucket (" << strerror(-rv) << ")";
}

TEST_F(FileSystemS3VirtualBucket, List) {
	S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr);
	TestDirectoryContents(fs, "/test/cells/tabula-sapiens");
}

TEST_F(FileSystemS3VirtualNoBucket, Stat) {
	S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr);
	struct stat buff;
	auto rv = fs.Stat(
		"/test/genome-browser/cells/tabula-sapiens/cellbrowser.json.bak",
		&buff);
	ASSERT_EQ(rv, 0) << "Failed to stat AWS bucket (" << strerror(-rv) << ")";
}

TEST_F(FileSystemS3VirtualNoBucket, List) {
	S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr);
	TestDirectoryContents(fs, "/test/genome-browser/cells/tabula-sapiens");
}

TEST_F(FileSystemS3PathBucket, Stat) {
	S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr);
	struct stat buff;
	auto rv = fs.Stat("/test/cells/tabula-sapiens/cellbrowser.json.bak", &buff);
	ASSERT_EQ(rv, 0) << "Failed to stat AWS bucket (" << strerror(-rv) << ")";
}

TEST_F(FileSystemS3PathBucket, List) {
	S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr);
	TestDirectoryContents(fs, "/test/cells/tabula-sapiens");
}

TEST_F(FileSystemS3PathNoBucket, Stat) {
	S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr);
	struct stat buff;
	auto rv = fs.Stat(
		"/test/genome-browser/cells/tabula-sapiens/cellbrowser.json.bak",
		&buff);
	ASSERT_EQ(rv, 0) << "Failed to stat AWS bucket (" << strerror(-rv) << ")";
}

TEST_F(FileSystemS3PathNoBucket, List) {
	S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr);
	TestDirectoryContents(fs, "/test/genome-browser/cells/tabula-sapiens/");
}

TEST_F(FileSystemS3PathBucketSlash, Stat) {
	S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr);
	struct stat buff;
	auto rv = fs.Stat("/test/cells/tabula-sapiens/cellbrowser.json.bak", &buff);
	ASSERT_EQ(rv, 0) << "Failed to stat AWS bucket (" << strerror(-rv) << ")";
}

TEST_F(FileSystemS3PathBucketSlash, List) {
	S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr);
	TestDirectoryContents(fs, "/test/cells/tabula-sapiens");
}

int main(int argc, char **argv) {
	auto logger = new XrdSysLogger(2, 0);
	auto log = new XrdSysError(logger, "curl_");
	AmazonRequest::Init(*log);
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
