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

//
// The tests in this file are meant to work with the minio-based fixture,
// meaning no internet connectivity is needed to run them.
//

#include "../src/S3Commands.hh"
#include "../src/S3FileSystem.hh"
#include "../src/shortfile.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdSys/XrdSysError.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <gtest/gtest.h>

#include <fcntl.h>
#include <fstream>
#include <iostream>

std::once_flag g_init_once;
std::string g_ca_file;
std::string g_minio_url;
std::string g_bucket_name;
std::string g_access_key_file;
std::string g_secret_key_file;

void parseEnvFile(const std::string &fname) {
	std::ifstream fh(fname);
	if (!fh.is_open()) {
		std::cerr << "Failed to open env file: " << strerror(errno);
		exit(1);
	}
	std::string line;
	while (std::getline(fh, line)) {
		auto idx = line.find("=");
		if (idx == std::string::npos) {
			continue;
		}
		auto key = line.substr(0, idx);
		auto val = line.substr(idx + 1);
		if (key == "X509_CA_FILE") {
			g_ca_file = val;
			setenv("X509_CERT_FILE", g_ca_file.c_str(), 1);
		} else if (key == "MINIO_URL") {
			g_minio_url = val;
		} else if (key == "BUCKET_NAME") {
			g_bucket_name = val;
		} else if (key == "ACCESS_KEY_FILE") {
			g_access_key_file = val;
		} else if (key == "SECRET_KEY_FILE") {
			g_secret_key_file = val;
		}
	}
}

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

TEST_F(FileSystemS3VirtualBucket, Create) {
	EXPECT_NO_THROW(
		{ S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr); });
}

TEST_F(FileSystemS3VirtualNoBucket, Create) {
	EXPECT_NO_THROW(
		{ S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr); });
}

TEST_F(FileSystemS3PathBucket, Create) {
	EXPECT_NO_THROW(
		{ S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr); });
}

TEST_F(FileSystemS3PathNoBucket, Create) {
	EXPECT_NO_THROW(
		{ S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr); });
}

TEST_F(FileSystemS3PathBucketSlash, Create) {
	EXPECT_NO_THROW(
		{ S3FileSystem fs(m_log.get(), m_configfn.c_str(), nullptr); });
}

// Tests where we query S3 test fixture
class FileSystemS3Fixture : public FileSystemFixtureBase {
	void SetUp() override {
		std::call_once(g_init_once, [&] {
			char *env_file = getenv("ENV_FILE");
			ASSERT_NE(env_file, nullptr) << "$ENV_FILE environment variable "
											"not set; required to run test";
			parseEnvFile(env_file);

			auto logger = new XrdSysLogger(2, 0);
			auto log = new XrdSysError(logger, "curl_");
			AmazonRequest::Init(*log);
		});

		FileSystemFixtureBase::SetUp();
	}

	virtual std::string GetConfig() override {
		return R"(
xrd.tlsca certfile )" +
			   g_ca_file + R"(
#s3.trace all dump
s3.trace all
s3.begin
s3.path_name        /test
s3.access_key_file  )" +
			   g_access_key_file + R"(
s3.secret_key_file  )" +
			   g_secret_key_file + R"(
s3.service_name     s3.example.com
s3.region           us-east-1
s3.bucket_name      )" +
			   g_bucket_name + R"(
s3.service_url      )" +
			   g_minio_url + R"(
s3.url_style        path
s3.end
	)";
	}

  public:
	void WritePattern(const std::string &name, const off_t writeSize,
					  const unsigned char chunkByte, const size_t chunkSize) {
		XrdSysLogger log;
		S3FileSystem fs(&log, m_configfn.c_str(), nullptr);

		std::unique_ptr<XrdOssDF> fh(fs.newFile());
		ASSERT_TRUE(fh);

		XrdOucEnv env;
		env.Put("oss.asize", std::to_string(writeSize).c_str());
		auto rv = fh->Open(name.c_str(), O_CREAT | O_WRONLY, 0755, env);
		ASSERT_EQ(rv, 0);

		size_t sizeToWrite = (static_cast<off_t>(chunkSize) >= writeSize)
								 ? static_cast<size_t>(writeSize)
								 : chunkSize;
		off_t curWriteSize = writeSize;
		auto curChunkByte = chunkByte;
		off_t offset = 0;
		while (sizeToWrite) {
			std::string writeBuffer(sizeToWrite, curChunkByte);

			rv = fh->Write(writeBuffer.data(), offset, sizeToWrite);
			ASSERT_EQ(rv, static_cast<ssize_t>(sizeToWrite));

			curWriteSize -= rv;
			offset += rv;
			sizeToWrite = (static_cast<off_t>(chunkSize) >= curWriteSize)
							  ? static_cast<size_t>(curWriteSize)
							  : chunkSize;
			curChunkByte += 1;
		}

		rv = fh->Close();
		ASSERT_EQ(rv, 0);

		VerifyContents(fs, name, writeSize, chunkByte, chunkSize);
	}

  private:
	void VerifyContents(S3FileSystem &fs, const std::string &obj,
						off_t expectedSize, unsigned char chunkByte,
						size_t chunkSize) {
		std::unique_ptr<XrdOssDF> fh(fs.newFile());
		ASSERT_TRUE(fh);

		XrdOucEnv env;
		auto rv = fh->Open(obj.c_str(), O_RDONLY, 0, env);
		ASSERT_EQ(rv, 0);

		size_t sizeToRead = (static_cast<off_t>(chunkSize) >= expectedSize)
								? expectedSize
								: chunkSize;
		unsigned char curChunkByte = chunkByte;
		off_t offset = 0;
		while (sizeToRead) {
			std::string readBuffer(sizeToRead, curChunkByte - 1);
			rv = fh->Read(readBuffer.data(), offset, sizeToRead);
			ASSERT_EQ(rv, static_cast<ssize_t>(sizeToRead));
			readBuffer.resize(rv);

			std::string correctBuffer(sizeToRead, curChunkByte);
			ASSERT_EQ(readBuffer, correctBuffer);

			expectedSize -= rv;
			offset += rv;
			sizeToRead = (static_cast<off_t>(chunkSize) >= expectedSize)
							 ? expectedSize
							 : chunkSize;
			curChunkByte += 1;
		}

		rv = fh->Close();
		ASSERT_EQ(rv, 0);
	}
};

// Upload a single byte into S3
TEST_F(FileSystemS3Fixture, UploadOneByte) {
	WritePattern("/test/write_one.txt", 1, 'X', 32 * 1024);
}

// Upload across multiple calls, single part
TEST_F(FileSystemS3Fixture, UploadMultipleCalls) {
	WritePattern("/test/write_alphabet.txt", 26, 'a', 1);
}

// Upload a zero-byte object
TEST_F(FileSystemS3Fixture, UploadZero) {
	WritePattern("/test/write_zero.txt", 0, 'X', 32 * 1024);
}

// Upload larger - two chunks.
TEST_F(FileSystemS3Fixture, UploadTwoChunks) {
	WritePattern("/test/write_two_chunks.txt", 1'024 + 42, 'a', 1'024);
}

// Upload larger - a few chunks.
TEST_F(FileSystemS3Fixture, UploadMultipleChunks) {
	WritePattern("/test/write_multi_chunks.txt", (10'000 / 1'024) * 1'024 + 42,
				 'a', 1'024);
}

// Upload across multiple parts, not aligned to partition.
TEST_F(FileSystemS3Fixture, UploadLarge) {
	WritePattern("/test/write_large_1.txt",
				 (100'000'000 / 1'310'720) * 1'310'720 + 42, 'a', 1'310'720);
}

// Upload a file into S3 that's the same size as the partition size
TEST_F(FileSystemS3Fixture, UploadLargePart) {
	WritePattern("/test/write_large_2.txt", 100'000'000, 'a', 131'072);
}

// Upload a small file where the partition size is aligned with the chunk size
TEST_F(FileSystemS3Fixture, UploadSmallAligned) {
	WritePattern("/test/write_large_3.txt", 1'000, 'a', 1'000);
}

// Upload a file into S3 that's the same size as the partition size, using
// chunks that align with the partition size
TEST_F(FileSystemS3Fixture, UploadLargePartAligned) {
	WritePattern("/test/write_large_4.txt", 100'000'000, 'a', 1'000'000);
}

// Upload a file into S3 resulting in multiple partitions
TEST_F(FileSystemS3Fixture, UploadMultiPartAligned) {
	WritePattern("/test/write_large_5.txt", 100'000'000, 'a', 1'000'000);
}

// Upload a file into S3 resulting in multiple partitioned using not-aligned
// chunks
TEST_F(FileSystemS3Fixture, UploadMultiPartUnaligned) {
	WritePattern("/test/write_large_1.txt", 100'000'000, 'a', 32'768);
}

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
