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

#include <XrdSys/XrdSysError.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <gtest/gtest.h>

class TestHTTPRequest : public HTTPRequest {
  public:
	XrdSysLogger log{};
	XrdSysError err{&log, "TestS3CommandsLog"};

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
	return RUN_ALL_TESTS();
}
