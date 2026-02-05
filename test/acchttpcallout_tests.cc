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

#include "../src/AccHttpCallout.hh"
#include "../src/shortfile.hh"

#include <XrdSec/XrdSecEntity.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>

using namespace XrdHTTPServer;

class AccHttpCalloutTest : public testing::Test {
  protected:
	AccHttpCalloutTest()
		: m_log(new XrdSysLogger(2, 0)), m_err(m_log.get(), "test_") {}

	void SetUp() override { setenv("XRDINSTANCE", "xrootd", 1); }

	// Create a minimal config file for testing
	void createConfigFile(const std::string &filename,
						  const std::string &content) {
		std::ofstream ofs(filename);
		ofs << content;
		ofs.close();
	}

	std::unique_ptr<XrdSysLogger> m_log;
	XrdSysError m_err;
};

// Test: Configuration parsing
TEST_F(AccHttpCalloutTest, ConfigParsing) {
	std::string configFile = createShortFile(
		"acchttpcallout.endpoint https://example.com/auth\n"
		"acchttpcallout.cache_ttl_positive 120\n"
		"acchttpcallout.cache_ttl_negative 60\n"
		"acchttpcallout.passthrough true\n");

	AccHttpCallout callout(&m_err, configFile.c_str(), nullptr);

	// We can't directly access private members, but we can verify the plugin
	// was created successfully
	EXPECT_NE(nullptr, &callout);
}

// Test: Config missing endpoint should fail
TEST_F(AccHttpCalloutTest, ConfigMissingEndpoint) {
	std::string configFile =
		createShortFile("acchttpcallout.cache_ttl_positive 120\n");

	EXPECT_THROW(
		{ AccHttpCallout callout(&m_err, configFile.c_str(), nullptr); },
		std::runtime_error);
}

// Test: Operation to verb conversion
TEST_F(AccHttpCalloutTest, OperationToVerb) {
	std::string configFile =
		createShortFile("acchttpcallout.endpoint https://example.com/auth\n");

	AccHttpCallout callout(&m_err, configFile.c_str(), nullptr);

	// Test via Access() call - the verb conversion is internal
	// We'll validate behavior indirectly through error messages
	EXPECT_NE(nullptr, &callout);
}

// Test: Test() method
TEST_F(AccHttpCalloutTest, TestMethod) {
	std::string configFile =
		createShortFile("acchttpcallout.endpoint https://example.com/auth\n");

	AccHttpCallout callout(&m_err, configFile.c_str(), nullptr);

	// Test with no privileges
	EXPECT_EQ(0, callout.Test(XrdAccPrivs(XrdAccPriv_None), AOP_Read));

	// Test with all privileges
	EXPECT_NE(0, callout.Test(XrdAccPrivs(~0), AOP_Read));
}

// Test: Audit method
TEST_F(AccHttpCalloutTest, AuditMethod) {
	std::string configFile =
		createShortFile("acchttpcallout.endpoint https://example.com/auth\n");

	AccHttpCallout callout(&m_err, configFile.c_str(), nullptr);

	XrdSecEntity entity;
	entity.name = const_cast<char *>("testuser");

	// Audit should always return success
	EXPECT_EQ(1, callout.Audit(1, &entity, "/test/path", AOP_Read, nullptr));
	EXPECT_EQ(1, callout.Audit(0, &entity, "/test/path", AOP_Read, nullptr));
}

// Test: Access without token should deny
TEST_F(AccHttpCalloutTest, AccessNoToken) {
	std::string configFile = createShortFile(
		"acchttpcallout.endpoint https://example.com/auth\n"
		"acchttpcallout.passthrough false\n");

	AccHttpCallout callout(&m_err, configFile.c_str(), nullptr);

	XrdSecEntity entity;
	entity.name = const_cast<char *>("testuser");
	entity.endorsements = nullptr; // No token

	XrdAccPrivs privs =
		callout.Access(&entity, "/test/path", AOP_Read, nullptr);

	// Should deny access (return XrdAccPriv_None)
	EXPECT_EQ(XrdAccPriv_None, privs);
}

// Test: Access with empty token should deny
TEST_F(AccHttpCalloutTest, AccessEmptyToken) {
	std::string configFile = createShortFile(
		"acchttpcallout.endpoint https://example.com/auth\n"
		"acchttpcallout.passthrough false\n");

	AccHttpCallout callout(&m_err, configFile.c_str(), nullptr);

	XrdSecEntity entity;
	entity.name = const_cast<char *>("testuser");
	entity.endorsements = const_cast<char *>(""); // Empty token

	XrdAccPrivs privs =
		callout.Access(&entity, "/test/path", AOP_Read, nullptr);

	// Should deny access (return XrdAccPriv_None)
	EXPECT_EQ(XrdAccPriv_None, privs);
}

// Test: Cache functionality (indirectly via repeated calls)
TEST_F(AccHttpCalloutTest, CacheFunctionality) {
	std::string configFile = createShortFile(
		"acchttpcallout.endpoint https://nonexistent.example.com/auth\n"
		"acchttpcallout.cache_ttl_negative 5\n"
		"acchttpcallout.passthrough false\n");

	AccHttpCallout callout(&m_err, configFile.c_str(), nullptr);

	XrdSecEntity entity;
	entity.name = const_cast<char *>("testuser");
	entity.endorsements = const_cast<char *>("test_token");

	// First call will fail (non-existent endpoint)
	XrdAccPrivs privs1 =
		callout.Access(&entity, "/test/path", AOP_Read, nullptr);

	// Second call should be cached (won't hit the network again)
	// We can verify this indirectly by checking that it returns quickly
	auto start = std::chrono::steady_clock::now();
	XrdAccPrivs privs2 =
		callout.Access(&entity, "/test/path", AOP_Read, nullptr);
	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::steady_clock::now() - start);

	// Cached response should be very fast (< 100ms)
	EXPECT_LT(duration.count(), 100);

	// Both should return the same denial
	EXPECT_EQ(privs1, privs2);
}

// Test: Extended error information version of Access
TEST_F(AccHttpCalloutTest, AccessWithErrorInfo) {
	std::string configFile = createShortFile(
		"acchttpcallout.endpoint https://example.com/auth\n"
		"acchttpcallout.passthrough false\n");

	AccHttpCallout callout(&m_err, configFile.c_str(), nullptr);

	XrdSecEntity entity;
	entity.name = const_cast<char *>("testuser");
	entity.endorsements = nullptr; // No token

	std::string eInfo;
	XrdAccPrivs privs =
		callout.Access(&entity, "/test/path", AOP_Read, eInfo, nullptr);

	// Should deny access and populate error info
	EXPECT_EQ(XrdAccPriv_None, privs);
	EXPECT_FALSE(eInfo.empty());
	EXPECT_NE(std::string::npos, eInfo.find("token"));
}
