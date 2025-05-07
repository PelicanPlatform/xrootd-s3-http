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
#include <gtest/gtest.h>

#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <string>

std::string g_ca_file;
std::string g_config_file;
std::string g_url;

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
		} else if (key == "XROOTD_URL") {
			g_url = val;
		} else if (key == "XROOTD_CFG") {
			g_config_file = val;
		}
	}
}

TEST(TestHTTPFile, TestXfer) {
	XrdSysLogger log;

	HTTPFileSystem fs(&log, g_config_file.c_str(), nullptr);

	struct stat si;
	auto rc = fs.Stat("/hello_world.txt", &si);
	ASSERT_EQ(rc, 0);
	ASSERT_EQ(si.st_size, 13);

	auto fh = fs.newFile();
	XrdOucEnv env;
	rc = fh->Open("/hello_world.txt", O_RDONLY, 0700, env);
	ASSERT_EQ(rc, 0);

	char buf[12];
	auto res = fh->Read(buf, 0, 12);
	ASSERT_EQ(res, 12);

	ASSERT_EQ(memcmp(buf, "Hello, World", 12), 0);

	ASSERT_EQ(fh->Close(), 0);
}

TEST(TestHTTPFile, TestMkdir) {
	XrdSysLogger log;

	HTTPFileSystem fs(&log, g_config_file.c_str(), nullptr);

	const auto ret = fs.Mkdir("/newdir", 0755);
	ASSERT_EQ(ret, 0);
}

class TestHTTPRequest : public HTTPRequest {
  public:
	XrdSysLogger log{};
	XrdSysError err{&log, "TestHTTPR3equest"};

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

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);

	if (argc != 2) {
		printf("Usage: %s test_env_file", argv[0]);
		return 1;
	}
	setenv("XRDINSTANCE", "xrootd", 1);
	std::cout << "Running HTTP test with environment file " << argv[1]
			  << std::endl;
	parseEnvFile(argv[1]);

	auto logger = new XrdSysLogger(2, 0);
	auto log = new XrdSysError(logger, "curl_");
	HTTPRequest::Init(*log);

	return RUN_ALL_TESTS();
}
