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

#include "../src/HTTPCommands.hh"
#include "../src/HTTPFileSystem.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdSys/XrdSysError.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <execinfo.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

std::string g_ca_file;
std::string g_config_file;
std::string g_url;

void parseEnvFile() {
	const char *fname = getenv("ENV_FILE");
	if (!fname) {
		std::cerr << "No env file specified" << std::endl;
		exit(1);
	}
	std::cout << "Using env file: " << fname << std::endl;

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
		} else if (key == "XROOTD_URL") {
			g_url = val;
		} else if (key == "XROOTD_CFG") {
			g_config_file = val;
		}
	}
}

TEST(TestHTTPFile, TestList) {
	XrdSysLogger log;

	XrdOucEnv env;
	HTTPFileSystem fs(&log, g_config_file.c_str(), &env);

	struct stat si;
	auto rc = fs.Stat("/testdir/", &si, 0, &env);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(si.st_size, 4096);

	auto fd_raw = fs.newDir();
	ASSERT_NE(fd_raw, nullptr);
	std::unique_ptr<XrdOssDF> fd(fd_raw);
	struct stat statStruct;
	fd->StatRet(&statStruct);

	rc = fd->Open("/testdir", O_RDONLY, 0700, env);
	ASSERT_EQ(rc, -21);
	ASSERT_EQ(fd->Opendir("/testdir", env), 0);

	char buf[255];
	auto res = fd->Readdir(buf, 255);
	ASSERT_EQ(res, 15);
}

TEST(TestHTTPFile, TestXfer) {
	XrdSysLogger log;
	HTTPFileSystem fs(&log, g_config_file.c_str(), nullptr);

	struct stat si;
	XrdOucEnv env;
	auto rc = fs.Stat("/hello_world.txt", &si, 0, &env);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(si.st_size, 13);

	std::unique_ptr<XrdOssDF> fh(fs.newFile());
	rc = fh->Open("/hello_world.txt", O_RDONLY, 0700, env);
	ASSERT_EQ(rc, 0);

	char buf[12];
	auto res = fh->Read(buf, 0, 12);
	ASSERT_EQ(res, 12);

	ASSERT_EQ(memcmp(buf, "Hello, World", 12), 0);

	ASSERT_EQ(fh->Close(), 0);
}

TEST(TestHTTPFile, TestWriteZeroByteFile) {
	XrdSysLogger log;
	HTTPFileSystem fs(&log, g_config_file.c_str(), nullptr);

	XrdOucEnv env;
	std::unique_ptr<XrdOssDF> fh(fs.newFile());
	// Create a 0-byte file
	auto rc =
		fh->Open("/empty_file.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644, env);
	ASSERT_EQ(rc, 0);

	// Close the file immediately (0 bytes written)
	ASSERT_EQ(fh->Close(), 0);

	// Verify the file exists and has 0 size
	struct stat si;
	rc = fs.Stat("/empty_file.txt", &si, 0, &env);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(si.st_size, 0);
}

TEST(TestHTTPFile, TestWriteSmallFile) {
	XrdSysLogger log;
	HTTPFileSystem fs(&log, g_config_file.c_str(), nullptr);

	XrdOucEnv env;
	std::unique_ptr<XrdOssDF> fh(fs.newFile());

	// Create a small file
	auto rc =
		fh->Open("/test_write.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644, env);
	ASSERT_EQ(rc, 0);

	// Write some test data
	const char test_data[] = "This is a test file for writing operations.";
	const size_t data_size = strlen(test_data);
	auto write_res = fh->Write(test_data, 0, data_size);
	ASSERT_EQ(write_res, static_cast<ssize_t>(data_size));

	ASSERT_EQ(fh->Close(), 0);

	// Verify the file was written correctly
	struct stat si;
	rc = fs.Stat("/test_write.txt", &si, 0, &env);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(si.st_size, data_size);

	// Read back the file to verify content
	std::unique_ptr<XrdOssDF> read_fh(fs.newFile());
	rc = read_fh->Open("/test_write.txt", O_RDONLY, 0700, env);
	ASSERT_EQ(rc, 0);

	char read_buf[256];
	auto read_res = read_fh->Read(read_buf, 0, data_size);
	ASSERT_EQ(read_res, static_cast<ssize_t>(data_size));
	ASSERT_EQ(memcmp(read_buf, test_data, data_size), 0);

	ASSERT_EQ(read_fh->Close(), 0);
}

TEST(TestHTTPFile, TestWriteLargeFile) {
	XrdSysLogger log;
	HTTPFileSystem fs(&log, g_config_file.c_str(), nullptr);

	XrdOucEnv env;
	std::unique_ptr<XrdOssDF> fh(fs.newFile());

	// Create a large file (2 MB)
	auto rc = fh->Open("/test_large_file.txt", O_WRONLY | O_CREAT | O_TRUNC,
					   0644, env);
	ASSERT_EQ(rc, 0);

	// Generate 2 MB of test data
	const size_t file_size = 2 * 1024 * 1024; // 2 MB
	std::vector<char> test_data(file_size);
	// Fill with a repeating pattern for easy verification
	for (size_t i = 0; i < file_size; i++) {
		test_data[i] = static_cast<char>(i % 256);
	}

	// Write the data in chunks to test streaming upload
	const size_t chunk_size = 64 * 1024; // 64 KB chunks
	size_t total_written = 0;

	for (size_t offset = 0; offset < file_size; offset += chunk_size) {
		size_t current_chunk_size = std::min(chunk_size, file_size - offset);
		auto write_res =
			fh->Write(&test_data[offset], offset, current_chunk_size);
		ASSERT_EQ(write_res, static_cast<ssize_t>(current_chunk_size));
		total_written += current_chunk_size;
	}

	ASSERT_EQ(total_written, file_size);
	ASSERT_EQ(fh->Close(), 0);

	// Verify the file was written correctly
	struct stat si;
	rc = fs.Stat("/test_large_file.txt", &si, 0, &env);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(si.st_size, file_size);

	// Read back the file to verify content
	std::unique_ptr<XrdOssDF> read_fh(fs.newFile());
	rc = read_fh->Open("/test_large_file.txt", O_RDONLY, 0700, env);
	ASSERT_EQ(rc, 0);

	// Read the data in chunks
	std::vector<char> read_buf(file_size);
	size_t total_read = 0;

	for (size_t offset = 0; offset < file_size; offset += chunk_size) {
		size_t current_chunk_size = std::min(chunk_size, file_size - offset);
		auto read_res =
			read_fh->Read(&read_buf[offset], offset, current_chunk_size);
		ASSERT_EQ(read_res, static_cast<ssize_t>(current_chunk_size));
		total_read += current_chunk_size;
	}

	ASSERT_EQ(total_read, file_size);
	ASSERT_EQ(memcmp(read_buf.data(), test_data.data(), file_size), 0);

	ASSERT_EQ(read_fh->Close(), 0);
}

class TestHTTPRequest : public HTTPRequest {
  public:
	XrdSysLogger log{};
	XrdSysError err{&log, "TestHTTPRequest"};

	TestHTTPRequest(const std::string &url) : HTTPRequest(url, err, nullptr) {}
};

TEST(TestHTTPParseProtocol, Test1) {
	const std::string httpURL = "https://my-test-url.com:443";
	TestHTTPRequest req{httpURL};

	// Test parsing of https
	std::string protocol;
	req.parseProtocol("https://my-test-url.com:443", protocol);
	ASSERT_EQ(protocol, "https");

	// Test parsing for http
	req.parseProtocol("http://my-test-url.com:443", protocol);
	ASSERT_EQ(protocol, "http");
}

void segfaultHandler(int sig) {
	void *array[20];
	size_t size;

	// Get void*'s for all entries on the stack
	size = backtrace(array, 20);

	// Print stack trace to stderr
	fprintf(stderr, "Error: signal %d:\n", sig);
	backtrace_symbols_fd(array, size, STDERR_FILENO);

	exit(1);
}

int main(int argc, char **argv) {

	::testing::InitGoogleTest(&argc, argv);

	setenv("XRDINSTANCE", "xrootd", 1);
	parseEnvFile();

	auto logger = new XrdSysLogger(2, 0);
	auto log = new XrdSysError(logger, "curl_");
	HTTPRequest::Init(*log);

	return RUN_ALL_TESTS();
}
