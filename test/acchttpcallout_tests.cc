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

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

using namespace XrdHTTPServer;

namespace {

// Write `content` to a unique temp file and return its path.
std::string writeTempConfig(const std::string &content) {
	char tmpl[] = "/tmp/acchttp_test_XXXXXX";
	int fd = mkstemp(tmpl);
	EXPECT_NE(fd, -1);
	auto n = write(fd, content.data(), content.size());
	EXPECT_EQ(n, static_cast<ssize_t>(content.size()));
	close(fd);
	return std::string(tmpl);
}

// A minimal HTTP server bound to 127.0.0.1 on an ephemeral port.  It answers
// every request with a preconfigured status code and body, records how many
// requests it received, and captures the most recent request line and
// Authorization header so tests can assert on what the plugin sent.
class MockHttpServer {
  public:
	MockHttpServer(int status, std::string body)
		: m_status(status), m_body(std::move(body)) {
		m_fd = socket(AF_INET, SOCK_STREAM, 0);
		EXPECT_GE(m_fd, 0);
		int one = 1;
		setsockopt(m_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

		sockaddr_in addr{};
		addr.sin_family = AF_INET;
		addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		addr.sin_port = 0; // ephemeral
		EXPECT_EQ(bind(m_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)),
				  0);
		EXPECT_EQ(listen(m_fd, 8), 0);

		socklen_t len = sizeof(addr);
		EXPECT_EQ(getsockname(m_fd, reinterpret_cast<sockaddr *>(&addr), &len),
				  0);
		m_port = ntohs(addr.sin_port);

		m_thread = std::thread([this] { Run(); });
	}

	~MockHttpServer() {
		m_running = false;
		shutdown(m_fd, SHUT_RDWR);
		close(m_fd);
		if (m_thread.joinable()) {
			m_thread.join();
		}
	}

	std::string endpoint() const {
		return "http://127.0.0.1:" + std::to_string(m_port) + "/auth";
	}

	int requestCount() const { return m_request_count.load(); }

	std::string lastRequestLine() {
		std::lock_guard<std::mutex> l(m_mutex);
		return m_last_request_line;
	}

	std::string lastAuthHeader() {
		std::lock_guard<std::mutex> l(m_mutex);
		return m_last_auth_header;
	}

  private:
	void Run() {
		while (m_running) {
			int client = accept(m_fd, nullptr, nullptr);
			if (client < 0) {
				if (!m_running) {
					break;
				}
				continue;
			}
			Handle(client);
			close(client);
		}
	}

	void Handle(int client) {
		std::string request;
		char buf[1024];
		// Read until the end of the request headers.
		while (request.find("\r\n\r\n") == std::string::npos) {
			ssize_t n = recv(client, buf, sizeof(buf), 0);
			if (n <= 0) {
				break;
			}
			request.append(buf, n);
		}

		{
			std::lock_guard<std::mutex> l(m_mutex);
			auto eol = request.find("\r\n");
			m_last_request_line =
				eol == std::string::npos ? request : request.substr(0, eol);
			m_last_auth_header.clear();
			auto ap = request.find("Authorization:");
			if (ap != std::string::npos) {
				auto end = request.find("\r\n", ap);
				m_last_auth_header = request.substr(
					ap, (end == std::string::npos ? request.size() : end) - ap);
			}
		}
		m_request_count.fetch_add(1);

		const char *reason = m_status == 200   ? "OK"
							 : m_status == 401 ? "Unauthorized"
							 : m_status == 403 ? "Forbidden"
											   : "Error";
		std::string resp =
			"HTTP/1.1 " + std::to_string(m_status) + " " + reason +
			"\r\nContent-Length: " + std::to_string(m_body.size()) +
			"\r\nConnection: close\r\n\r\n" + m_body;
		ssize_t off = 0;
		while (off < static_cast<ssize_t>(resp.size())) {
			ssize_t n = send(client, resp.data() + off, resp.size() - off, 0);
			if (n <= 0) {
				break;
			}
			off += n;
		}
	}

	int m_fd{-1};
	int m_port{0};
	int m_status;
	std::string m_body;
	std::atomic<bool> m_running{true};
	std::atomic<int> m_request_count{0};
	std::thread m_thread;
	std::mutex m_mutex;
	std::string m_last_request_line;
	std::string m_last_auth_header;
};

XrdSecEntity makeEntity(const char *token) {
	XrdSecEntity e;
	e.name = const_cast<char *>("testuser");
	e.endorsements = const_cast<char *>(token);
	return e;
}

// A stand-in for a chained authorization object; records how many times it was
// consulted and returns a fixed verdict.
class MockChain : public XrdAccAuthorize {
  public:
	XrdAccPrivs Access(const XrdSecEntity *, const char *,
					   const Access_Operation, XrdOucEnv * = 0) override {
		m_calls.fetch_add(1);
		return m_verdict;
	}
	int Audit(const int, const XrdSecEntity *, const char *,
			  const Access_Operation, XrdOucEnv * = 0) override {
		return 0;
	}
	int Test(const XrdAccPrivs, const Access_Operation) override { return 0; }

	std::atomic<int> m_calls{0};
	XrdAccPrivs m_verdict{XrdAccPrivs(~0)};
};

} // namespace

class AccHttpCalloutTest : public testing::Test {
  protected:
	AccHttpCalloutTest()
		: m_log(new XrdSysLogger(2, 0)), m_err(m_log.get(), "test_") {}

	void SetUp() override { setenv("XRDINSTANCE", "xrootd", 1); }

	std::unique_ptr<AccHttpCallout>
	makeCallout(const std::string &endpoint, const std::string &extra = "",
				XrdAccAuthorize *chain = nullptr) {
		std::string cfg = "httpcallout.endpoint " + endpoint + "\n" + extra;
		std::string file = writeTempConfig(cfg);
		auto c = std::make_unique<AccHttpCallout>(&m_err, file.c_str(), nullptr,
												  chain);
		unlink(file.c_str());
		return c;
	}

	std::unique_ptr<XrdSysLogger> m_log;
	XrdSysError m_err;
};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

TEST_F(AccHttpCalloutTest, ConfigParsing) {
	std::string file =
		writeTempConfig("httpcallout.endpoint https://example.com/auth\n"
						"httpcallout.cache_ttl_positive 120\n"
						"httpcallout.cache_ttl_negative 60\n"
						"httpcallout.passthrough true\n");
	// Constructs without throwing == config parsed and endpoint accepted.
	EXPECT_NO_THROW({ AccHttpCallout c(&m_err, file.c_str(), nullptr); });
	unlink(file.c_str());
}

TEST_F(AccHttpCalloutTest, ConfigMissingEndpoint) {
	std::string file = writeTempConfig("httpcallout.cache_ttl_positive 120\n");
	EXPECT_THROW(
		{ AccHttpCallout c(&m_err, file.c_str(), nullptr); },
		std::runtime_error);
	unlink(file.c_str());
}

// ---------------------------------------------------------------------------
// Non-network behavior
// ---------------------------------------------------------------------------

TEST_F(AccHttpCalloutTest, TestMethod) {
	auto c = makeCallout("https://example.com/auth");
	EXPECT_EQ(0, c->Test(XrdAccPrivs(XrdAccPriv_None), AOP_Read));
	EXPECT_NE(0, c->Test(XrdAccPrivs(~0), AOP_Read));
}

TEST_F(AccHttpCalloutTest, AuditMethod) {
	auto c = makeCallout("https://example.com/auth");
	XrdSecEntity entity = makeEntity("tok");
	EXPECT_EQ(1, c->Audit(1, &entity, "/test/path", AOP_Read, nullptr));
	EXPECT_EQ(1, c->Audit(0, &entity, "/test/path", AOP_Read, nullptr));
}

TEST_F(AccHttpCalloutTest, AccessNoTokenDenies) {
	auto c = makeCallout("https://example.com/auth");
	XrdSecEntity entity = makeEntity(nullptr);
	std::string eInfo;
	EXPECT_EQ(XrdAccPriv_None,
			  c->Access(&entity, "/test/path", AOP_Read, eInfo, nullptr));
	EXPECT_NE(std::string::npos, eInfo.find("token"));
}

TEST_F(AccHttpCalloutTest, AccessEmptyTokenDenies) {
	auto c = makeCallout("https://example.com/auth");
	XrdSecEntity entity = makeEntity("");
	EXPECT_EQ(XrdAccPriv_None,
			  c->Access(&entity, "/test/path", AOP_Read, nullptr));
}

// ---------------------------------------------------------------------------
// HTTP callout behavior (against an in-process mock server)
// ---------------------------------------------------------------------------

TEST_F(AccHttpCalloutTest, CalloutGrantsOn200) {
	MockHttpServer server(200, "");
	auto c = makeCallout(server.endpoint());
	XrdSecEntity entity = makeEntity("mytoken");

	XrdAccPrivs privs = c->Access(&entity, "/store/data", AOP_Read, nullptr);
	EXPECT_NE(XrdAccPriv_None, privs);
	EXPECT_EQ(1, server.requestCount());

	// The plugin must send the path and verb as query params and the token as
	// a bearer Authorization header.
	EXPECT_NE(std::string::npos, server.lastRequestLine().find("path="));
	EXPECT_NE(std::string::npos, server.lastRequestLine().find("verb=GET"));
	EXPECT_NE(std::string::npos,
			  server.lastAuthHeader().find("Bearer mytoken"));
}

TEST_F(AccHttpCalloutTest, CalloutDeniesOn403) {
	MockHttpServer server(403, "");
	auto c = makeCallout(server.endpoint());
	XrdSecEntity entity = makeEntity("mytoken");

	EXPECT_EQ(XrdAccPriv_None,
			  c->Access(&entity, "/store/data", AOP_Read, nullptr));
	EXPECT_EQ(1, server.requestCount());
}

TEST_F(AccHttpCalloutTest, CalloutDeniesWithErrorInfoOn500) {
	MockHttpServer server(500, "");
	auto c = makeCallout(server.endpoint());
	XrdSecEntity entity = makeEntity("mytoken");

	std::string eInfo;
	EXPECT_EQ(XrdAccPriv_None,
			  c->Access(&entity, "/store/data", AOP_Read, eInfo, nullptr));
	EXPECT_FALSE(eInfo.empty());
}

TEST_F(AccHttpCalloutTest, VerbMappingReflectsOperation) {
	MockHttpServer server(200, "");
	auto c = makeCallout(server.endpoint());
	XrdSecEntity entity = makeEntity("mytoken");

	c->Access(&entity, "/store/data", AOP_Create, nullptr);
	EXPECT_NE(std::string::npos, server.lastRequestLine().find("verb=PUT"));

	c->Access(&entity, "/store/dir", AOP_Mkdir, nullptr);
	EXPECT_NE(std::string::npos, server.lastRequestLine().find("verb=MKCOL"));
}

// ---------------------------------------------------------------------------
// Caching
// ---------------------------------------------------------------------------

TEST_F(AccHttpCalloutTest, PositiveResponseIsCached) {
	MockHttpServer server(200, "");
	auto c =
		makeCallout(server.endpoint(), "httpcallout.cache_ttl_positive 60\n");
	XrdSecEntity entity = makeEntity("mytoken");

	EXPECT_NE(XrdAccPriv_None,
			  c->Access(&entity, "/store/data", AOP_Read, nullptr));
	EXPECT_NE(XrdAccPriv_None,
			  c->Access(&entity, "/store/data", AOP_Read, nullptr));

	// The second identical request must be served from cache: exactly one
	// callout reached the server.
	EXPECT_EQ(1, server.requestCount());
}

TEST_F(AccHttpCalloutTest, NegativeResponseIsCached) {
	MockHttpServer server(403, "");
	auto c =
		makeCallout(server.endpoint(), "httpcallout.cache_ttl_negative 60\n");
	XrdSecEntity entity = makeEntity("mytoken");

	EXPECT_EQ(XrdAccPriv_None,
			  c->Access(&entity, "/store/data", AOP_Read, nullptr));
	EXPECT_EQ(XrdAccPriv_None,
			  c->Access(&entity, "/store/data", AOP_Read, nullptr));
	EXPECT_EQ(1, server.requestCount());
}

TEST_F(AccHttpCalloutTest, DifferentPathsAreNotConflated) {
	MockHttpServer server(200, "");
	auto c = makeCallout(server.endpoint());
	XrdSecEntity entity = makeEntity("mytoken");

	c->Access(&entity, "/store/a", AOP_Read, nullptr);
	c->Access(&entity, "/store/b", AOP_Read, nullptr);
	EXPECT_EQ(2, server.requestCount());
}

// ---------------------------------------------------------------------------
// JSON response parsing
// ---------------------------------------------------------------------------

TEST_F(AccHttpCalloutTest, JsonResponseIsAccepted) {
	MockHttpServer server(200, R"({"user":"jdoe","group":"physicists",)"
							   R"("authorizations":[{"verb":"GET",)"
							   R"("prefixes":["/store/data"]}]})");
	auto c = makeCallout(server.endpoint());
	XrdSecEntity entity = makeEntity("mytoken");

	EXPECT_NE(XrdAccPriv_None,
			  c->Access(&entity, "/store/data", AOP_Read, nullptr));
	EXPECT_EQ(1, server.requestCount());
}

TEST_F(AccHttpCalloutTest, MalformedJsonStillGrantsOn200) {
	MockHttpServer server(200, "this is not json");
	auto c = makeCallout(server.endpoint());
	XrdSecEntity entity = makeEntity("mytoken");

	// A 200 grants regardless of whether the optional JSON body parses.
	EXPECT_NE(XrdAccPriv_None,
			  c->Access(&entity, "/store/data", AOP_Read, nullptr));
}

// ---------------------------------------------------------------------------
// Token extraction
// ---------------------------------------------------------------------------

TEST_F(AccHttpCalloutTest, TokenFromAuthzEnv) {
	MockHttpServer server(200, "");
	auto c = makeCallout(server.endpoint());
	XrdSecEntity entity = makeEntity(nullptr); // no endorsements
	XrdOucEnv env("authz=Bearer%20envtoken");

	EXPECT_NE(XrdAccPriv_None,
			  c->Access(&entity, "/store/data", AOP_Read, &env));
	EXPECT_EQ(1, server.requestCount());
	// The "Bearer%20" prefix is stripped and the bare token forwarded.
	EXPECT_NE(std::string::npos,
			  server.lastAuthHeader().find("Bearer envtoken"));
}

TEST_F(AccHttpCalloutTest, TokenFromCreds) {
	MockHttpServer server(200, "");
	auto c = makeCallout(server.endpoint());
	XrdSecEntity entity = makeEntity(nullptr);
	entity.creds = const_cast<char *>("credtoken");
	entity.credslen = 9;

	EXPECT_NE(XrdAccPriv_None,
			  c->Access(&entity, "/store/data", AOP_Read, nullptr));
	EXPECT_NE(std::string::npos,
			  server.lastAuthHeader().find("Bearer credtoken"));
}

// ---------------------------------------------------------------------------
// Prefix-authorization caching
// ---------------------------------------------------------------------------

TEST_F(AccHttpCalloutTest, PrefixAuthorizationCachesSubPaths) {
	MockHttpServer server(
		200,
		R"({"authorizations":[{"verb":"GET","prefixes":["/store/data"]}]})");
	auto c = makeCallout(server.endpoint());
	XrdSecEntity entity = makeEntity("mytoken");

	// First call to the prefix itself performs a callout.
	EXPECT_NE(XrdAccPriv_None,
			  c->Access(&entity, "/store/data", AOP_Read, nullptr));
	EXPECT_EQ(1, server.requestCount());

	// A sub-path with the same operation is served from the prefix cache.
	EXPECT_NE(XrdAccPriv_None,
			  c->Access(&entity, "/store/data/file.root", AOP_Read, nullptr));
	EXPECT_EQ(1, server.requestCount());

	// A different operation on the same sub-path is not covered by the GET
	// prefix rule and triggers a fresh callout.
	c->Access(&entity, "/store/data/file.root", AOP_Create, nullptr);
	EXPECT_EQ(2, server.requestCount());

	// A path outside the prefix triggers a fresh callout.
	c->Access(&entity, "/other/file", AOP_Read, nullptr);
	EXPECT_EQ(3, server.requestCount());
}

TEST_F(AccHttpCalloutTest, PrefixDoesNotMatchPartialComponent) {
	MockHttpServer server(
		200,
		R"({"authorizations":[{"verb":"GET","prefixes":["/store/data"]}]})");
	auto c = makeCallout(server.endpoint());
	XrdSecEntity entity = makeEntity("mytoken");

	c->Access(&entity, "/store/data", AOP_Read, nullptr);
	EXPECT_EQ(1, server.requestCount());

	// "/store/database" must NOT be treated as under "/store/data".
	c->Access(&entity, "/store/database", AOP_Read, nullptr);
	EXPECT_EQ(2, server.requestCount());
}

// ---------------------------------------------------------------------------
// Passthrough / chaining to the next authorization object
// ---------------------------------------------------------------------------

TEST_F(AccHttpCalloutTest, PassthroughDelegatesToChainOnNoToken) {
	MockChain chain;
	auto c = makeCallout("https://example.com/auth",
						 "httpcallout.passthrough true\n", &chain);
	XrdSecEntity entity = makeEntity(nullptr); // no token

	XrdAccPrivs privs = c->Access(&entity, "/p", AOP_Read, nullptr);
	EXPECT_EQ(1, chain.m_calls.load());
	EXPECT_NE(XrdAccPriv_None, privs); // chain granted
}

TEST_F(AccHttpCalloutTest, PassthroughDelegatesToChainOnDeny) {
	MockHttpServer server(403, "");
	MockChain chain;
	auto c = makeCallout(server.endpoint(), "httpcallout.passthrough true\n",
						 &chain);
	XrdSecEntity entity = makeEntity("mytoken");

	c->Access(&entity, "/p", AOP_Read, nullptr);
	EXPECT_EQ(1, server.requestCount()); // service consulted (403)
	EXPECT_EQ(1, chain.m_calls.load());	 // then delegated to the chain
}

TEST_F(AccHttpCalloutTest, NoPassthroughDeniesWithoutConsultingChain) {
	MockHttpServer server(403, "");
	MockChain chain;
	auto c = makeCallout(server.endpoint(), "httpcallout.passthrough false\n",
						 &chain);
	XrdSecEntity entity = makeEntity("mytoken");

	EXPECT_EQ(XrdAccPriv_None, c->Access(&entity, "/p", AOP_Read, nullptr));
	EXPECT_EQ(0, chain.m_calls.load()); // chain not consulted
}
