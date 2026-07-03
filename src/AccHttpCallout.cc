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

#include "AccHttpCallout.hh"

#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucGatherConf.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdVersion.hh>

#include <nlohmann/json.hpp>

#include <curl/curl.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <iomanip>
#include <mutex>
#include <openssl/sha.h>
#include <sstream>

using namespace XrdHTTPServer;

XrdVERSIONINFO(XrdAccAuthorizeObjAdd, AccHttpCallout);

extern "C" {
// XRootD loads authorization plugins with the ofs.authlib directive and calls
// XrdAccAuthorizeObjAdd, passing the previously-configured authorization object
// as `accP`.  We keep that object as the delegation target for passthrough
// (see httpcallout.passthrough), the same way the SciTokens plugin chains.
XrdAccAuthorize *XrdAccAuthorizeObjAdd(XrdSysLogger *lp, const char *cfn,
									   const char *parm, XrdOucEnv *envP,
									   XrdAccAuthorize *accP) {
	XrdSysError eDest(lp, "acchttpcallout");
	eDest.Say("Copr. 2025 Pelican Project, AccHttpCallout plugin v 1.0");

	if (parm) {
		eDest.Say("AccHttpCallout: Params: ", parm);
	}

	try {
		return new AccHttpCallout(&eDest, cfn, parm, accP);
	} catch (const std::exception &e) {
		eDest.Say("AccHttpCallout: Failed to initialize: ", e.what());
		return nullptr;
	}
}
}

// Helper function for curl write callback
static size_t WriteCallback(void *contents, size_t size, size_t nmemb,
							void *userp) {
	((std::string *)userp)->append((char *)contents, size * nmemb);
	return size * nmemb;
}

// Helper function to URL-encode a string
static std::string urlEncode(const std::string &value) {
	std::ostringstream escaped;
	for (char c : value) {
		if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
			escaped << c;
		} else {
			escaped << '%' << std::uppercase << std::hex
					<< int((unsigned char)c);
		}
	}
	return escaped.str();
}

AccHttpCallout::AccHttpCallout(XrdSysError *lp, const char *confg,
							   const char *parms, XrdAccAuthorize *chain)
	: m_chain(chain), m_last_cleanup(std::chrono::steady_clock::now()),
	  m_eDest(lp) {
	if (confg && !Config(confg)) {
		throw std::runtime_error("Failed to configure AccHttpCallout");
	}

	if (m_endpoint.empty()) {
		throw std::runtime_error(
			"AccHttpCallout: httpcallout.endpoint must be configured");
	}

	// libcurl must be globally initialized exactly once per process from a
	// single-threaded context; other plugins in this process (e.g. the HTTP
	// OSS) also use libcurl, so we neither re-init per instance nor call
	// curl_global_cleanup() here (which would pull the rug out from under
	// them).  A std::once_flag guards the one-time init.
	static std::once_flag curl_init_flag;
	std::call_once(curl_init_flag, [] { curl_global_init(CURL_GLOBAL_ALL); });
}

AccHttpCallout::~AccHttpCallout() {}

bool AccHttpCallout::Config(const char *configfn) {
	XrdOucGatherConf conf("httpcallout.", m_eDest);
	if (conf.Gather(configfn, XrdOucGatherConf::full_lines) < 0) {
		m_eDest->Say("AccHttpCallout: Failed to gather configuration");
		return false;
	}

	char *directive;
	while (conf.GetLine()) {
		directive = conf.GetToken();
		if (!directive) {
			continue;
		}

		if (!strcmp(directive, "httpcallout.endpoint")) {
			char *val = conf.GetToken();
			if (val) {
				m_endpoint = val;
				m_eDest->Say("AccHttpCallout: Endpoint set to: ",
							 m_endpoint.c_str());
			}
		} else if (!strcmp(directive, "httpcallout.cache_ttl_positive")) {
			char *val = conf.GetToken();
			if (val) {
				m_cache_ttl_positive = std::atoi(val);
				m_eDest->Say("AccHttpCallout: Positive cache TTL set to: ",
							 std::to_string(m_cache_ttl_positive).c_str(),
							 " seconds");
			}
		} else if (!strcmp(directive, "httpcallout.cache_ttl_negative")) {
			char *val = conf.GetToken();
			if (val) {
				m_cache_ttl_negative = std::atoi(val);
				m_eDest->Say("AccHttpCallout: Negative cache TTL set to: ",
							 std::to_string(m_cache_ttl_negative).c_str(),
							 " seconds");
			}
		} else if (!strcmp(directive, "httpcallout.passthrough")) {
			char *val = conf.GetToken();
			m_passthrough = val && (!strcmp(val, "true") || !strcmp(val, "1"));
			m_eDest->Say("AccHttpCallout: Passthrough set to: ",
						 m_passthrough ? "true" : "false");
		} else if (!strcmp(directive, "httpcallout.trace")) {
			char *level = conf.GetToken();
			if (level) {
				m_eDest->Say("AccHttpCallout: Trace level: ", level);
			}
		}
	}

	return true;
}

XrdAccPrivs AccHttpCallout::Access(const XrdSecEntity *Entity, const char *path,
								   const Access_Operation oper,
								   XrdOucEnv *Env) {
	std::string eInfo;
	return Access(Entity, path, oper, eInfo, Env);
}

XrdAccPrivs AccHttpCallout::Access(const XrdSecEntity *Entity, const char *path,
								   const Access_Operation oper,
								   std::string &eInfo, XrdOucEnv *Env) {
	const std::string pathStr = path ? path : "";

	std::string token = extractToken(Entity, Env);
	if (token.empty()) {
		eInfo = "No bearer token provided";
		m_eDest->Say("AccHttpCallout: No bearer token for path: ",
					 pathStr.c_str());
		return onFailure(Entity, path, oper, Env);
	}

	const std::string tokenKey = hashToken(token);
	const std::string verb = operationToVerb(oper);
	const std::string cacheKey = generateCacheKey(token, pathStr, oper);

	// Exact-decision cache: an earlier callout for this exact (token, path,
	// op).  A cached non-grant still runs the failure policy.
	CacheEntry entry;
	if (lookupCache(cacheKey, entry)) {
		m_eDest->Say("AccHttpCallout: Cache hit for path: ", pathStr.c_str());
		if (entry.privileges != XrdAccPriv_None) {
			return entry.privileges;
		}
		return onFailure(Entity, path, oper, Env);
	}

	// Prefix-rule cache: a previous response authorized a parent path for this
	// token and operation, so we can grant without another callout.
	if (lookupPrefixCache(tokenKey, pathStr, oper)) {
		m_eDest->Say("AccHttpCallout: Prefix cache hit for path: ",
					 pathStr.c_str());
		return XrdAccPrivs(~0);
	}

	// Make the HTTP callout.
	std::vector<AuthInfo> authInfos;
	std::string userInfo, groupInfo;
	int statusCode = makeHttpCallout(token, pathStr, verb, eInfo, authInfos,
									 userInfo, groupInfo);

	if (statusCode == 200) {
		storeCache(cacheKey, XrdAccPrivs(~0), m_cache_ttl_positive, userInfo,
				   groupInfo);
		// Cache any path-prefix authorizations for related sub-paths.
		storePrefixRules(tokenKey, authInfos, m_cache_ttl_positive);
		maybeCleanCache();
		return XrdAccPrivs(~0);
	}

	if (statusCode == 401 || statusCode == 403) {
		// Cache the denial so repeated attempts don't hammer the service, then
		// apply the failure policy (deny, or passthrough to the chain).
		storeCache(cacheKey, XrdAccPrivs(XrdAccPriv_None), m_cache_ttl_negative,
				   "", "");
		maybeCleanCache();
		return onFailure(Entity, path, oper, Env);
	}

	// 5xx / transport error: treat as a plugin error and do not cache (the
	// condition is expected to be transient).
	eInfo = "Authorization service error: " + std::to_string(statusCode);
	m_eDest->Say("AccHttpCallout: HTTP error ",
				 std::to_string(statusCode).c_str(),
				 " for path: ", pathStr.c_str());
	return onFailure(Entity, path, oper, Env);
}

XrdAccPrivs AccHttpCallout::onFailure(const XrdSecEntity *Entity,
									  const char *path,
									  const Access_Operation oper,
									  XrdOucEnv *Env) {
	if (m_passthrough && m_chain) {
		return m_chain->Access(Entity, path, oper, Env);
	}
	return XrdAccPrivs(XrdAccPriv_None);
}

std::string AccHttpCallout::extractToken(const XrdSecEntity *Entity,
										 XrdOucEnv *Env) const {
	// Primary location: the `authz` CGI value.  XrdHttp maps the incoming
	// Authorization header to this via `http.header2cgi Authorization authz`.
	// The value may retain a "Bearer " / "Bearer%20" prefix.
	const char *authz = Env ? Env->Get("authz") : nullptr;
	if (authz && !strncmp(authz, "Bearer%20", 9)) {
		authz += 9;
	} else if (authz && !strncmp(authz, "Bearer ", 7)) {
		authz += 7;
	}
	if (authz && *authz) {
		return std::string(authz);
	}

	// Fallback: the token carried in the security entity's credentials (e.g.
	// the ZTN token protocol).
	if (Entity && Entity->creds && Entity->credslen > 0) {
		return std::string(Entity->creds, Entity->credslen);
	}

	// Last resort: some flows place the token in `endorsements`.
	if (Entity && Entity->endorsements && *Entity->endorsements) {
		return std::string(Entity->endorsements);
	}

	return std::string();
}

int AccHttpCallout::Audit(const int accok, const XrdSecEntity *Entity,
						  const char *path, const Access_Operation oper,
						  XrdOucEnv *Env) {
	// Simple audit logging
	const char *result = accok ? "GRANTED" : "DENIED";
	const char *user = Entity && Entity->name ? Entity->name : "unknown";
	std::string verb = operationToVerb(oper);

	std::string msg = std::string("Audit: ") + result + " user=" + user +
					  " path=" + (path ? path : "") + " verb=" + verb;
	m_eDest->Say("AccHttpCallout: ", msg.c_str());

	return 1;
}

int AccHttpCallout::Test(const XrdAccPrivs priv, const Access_Operation oper) {
	// Simple test: if any privileges are set, allow the operation
	// A more sophisticated implementation would check specific privileges
	return priv != XrdAccPriv_None;
}

int AccHttpCallout::makeHttpCallout(const std::string &token,
									const std::string &path,
									const std::string &verb, std::string &eInfo,
									std::vector<AuthInfo> &authInfos,
									std::string &userInfo,
									std::string &groupInfo) {
	CURL *curl = curl_easy_init();
	if (!curl) {
		eInfo = "Failed to initialize CURL";
		return 500;
	}

	// Build URL with query parameters
	std::string url =
		m_endpoint + "?path=" + urlEncode(path) + "&verb=" + urlEncode(verb);

	std::string response;
	struct curl_slist *headers = nullptr;

	// Add Authorization header
	std::string authHeader = "Authorization: Bearer " + token;
	headers = curl_slist_append(headers, authHeader.c_str());

	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);

	CURLcode res = curl_easy_perform(curl);
	long statusCode = 0;
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &statusCode);

	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	if (res != CURLE_OK) {
		eInfo = "CURL error: " + std::string(curl_easy_strerror(res));
		return 500;
	}

	// Parse JSON response if status is 200
	if (statusCode == 200 && !response.empty()) {
		try {
			auto json = nlohmann::json::parse(response);

			// Extract authorizations
			if (json.contains("authorizations")) {
				for (const auto &auth : json["authorizations"]) {
					AuthInfo info;
					if (auth.contains("verb")) {
						info.verb = auth["verb"].get<std::string>();
					}
					if (auth.contains("prefixes")) {
						for (const auto &prefix : auth["prefixes"]) {
							info.prefixes.push_back(prefix.get<std::string>());
						}
					}
					authInfos.push_back(info);
				}
			}

			// Extract user and group info
			if (json.contains("user")) {
				userInfo = json["user"].get<std::string>();
			}
			if (json.contains("group")) {
				groupInfo = json["group"].get<std::string>();
			}
		} catch (const nlohmann::json::exception &e) {
			m_eDest->Say("AccHttpCallout: Failed to parse JSON response: ",
						 e.what());
		}
	}

	return static_cast<int>(statusCode);
}

std::string AccHttpCallout::operationToVerb(const Access_Operation oper) {
	switch (oper) {
	case AOP_Read:
		return "GET";
	case AOP_Readdir:
		return "PROPFIND";
	case AOP_Stat:
		return "HEAD";
	case AOP_Update:
	case AOP_Create:
		return "PUT";
	case AOP_Delete:
		return "DELETE";
	case AOP_Mkdir:
		return "MKCOL";
	case AOP_Rename:
	case AOP_Insert:
		return "MOVE";
	default:
		return "GET";
	}
}

Access_Operation AccHttpCallout::verbToOperation(const std::string &verb) {
	if (verb == "GET")
		return AOP_Read;
	if (verb == "PROPFIND")
		return AOP_Readdir;
	if (verb == "HEAD")
		return AOP_Stat;
	if (verb == "PUT")
		return AOP_Update;
	if (verb == "DELETE")
		return AOP_Delete;
	if (verb == "MKCOL")
		return AOP_Mkdir;
	if (verb == "MOVE")
		return AOP_Rename;
	return AOP_Read;
}

std::string AccHttpCallout::generateCacheKey(const std::string &token,
											 const std::string &path,
											 const Access_Operation oper) {
	// Use SHA256 to generate a cache key from token + path + operation
	std::string data =
		token + ":" + path + ":" + std::to_string(static_cast<int>(oper));

	unsigned char hash[SHA256_DIGEST_LENGTH];
	SHA256(reinterpret_cast<const unsigned char *>(data.c_str()), data.size(),
		   hash);

	std::ostringstream oss;
	for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
		oss << std::hex << std::setw(2) << std::setfill('0')
			<< static_cast<int>(hash[i]);
	}
	return oss.str();
}

std::string AccHttpCallout::hashToken(const std::string &token) {
	// The prefix-rule table is keyed by a hash of the token so raw tokens are
	// not retained as map keys.
	unsigned char hash[SHA256_DIGEST_LENGTH];
	SHA256(reinterpret_cast<const unsigned char *>(token.c_str()), token.size(),
		   hash);
	std::ostringstream oss;
	for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
		oss << std::hex << std::setw(2) << std::setfill('0')
			<< static_cast<int>(hash[i]);
	}
	return oss.str();
}

bool AccHttpCallout::pathHasPrefix(const std::string &path,
								   const std::string &prefix) {
	// Normalize a single trailing slash off the prefix (e.g. "/store/" and
	// "/store" behave identically).
	std::string p = prefix;
	if (p.size() > 1 && p.back() == '/') {
		p.pop_back();
	}
	if (p.empty() || p == "/") {
		return true; // authorizes everything
	}
	if (path.compare(0, p.size(), p) != 0) {
		return false;
	}
	// Require a component boundary so "/a/b" does not match "/a/bc".
	return path.size() == p.size() || path[p.size()] == '/';
}

bool AccHttpCallout::lookupPrefixCache(const std::string &tokenKey,
									   const std::string &path,
									   const Access_Operation oper) {
	std::lock_guard<std::mutex> lock(m_cache_mutex);

	auto it = m_prefix_cache.find(tokenKey);
	if (it == m_prefix_cache.end()) {
		return false;
	}

	auto now = std::chrono::steady_clock::now();
	for (const auto &rule : it->second) {
		if (now < rule.expiration && rule.oper == oper &&
			pathHasPrefix(path, rule.prefix)) {
			return true;
		}
	}
	return false;
}

void AccHttpCallout::storePrefixRules(const std::string &tokenKey,
									  const std::vector<AuthInfo> &authInfos,
									  int ttl) {
	if (authInfos.empty()) {
		return;
	}
	auto expiration =
		std::chrono::steady_clock::now() + std::chrono::seconds(ttl);

	std::lock_guard<std::mutex> lock(m_cache_mutex);
	auto &rules = m_prefix_cache[tokenKey];
	for (const auto &info : authInfos) {
		Access_Operation op = verbToOperation(info.verb);
		for (const auto &prefix : info.prefixes) {
			rules.push_back(PrefixRule{op, prefix, expiration});
		}
	}
}

bool AccHttpCallout::lookupCache(const std::string &key, CacheEntry &entry) {
	std::lock_guard<std::mutex> lock(m_cache_mutex);

	auto it = m_cache.find(key);
	if (it == m_cache.end()) {
		return false;
	}

	// Check if expired
	auto now = std::chrono::steady_clock::now();
	if (now >= it->second.expiration) {
		m_cache.erase(it);
		return false;
	}

	entry = it->second;
	return true;
}

void AccHttpCallout::storeCache(const std::string &key,
								const XrdAccPrivs privileges, int ttl,
								const std::string &userInfo,
								const std::string &groupInfo) {
	std::lock_guard<std::mutex> lock(m_cache_mutex);

	CacheEntry entry;
	entry.privileges = privileges;
	entry.expiration =
		std::chrono::steady_clock::now() + std::chrono::seconds(ttl);
	entry.userInfo = userInfo;
	entry.groupInfo = groupInfo;

	m_cache[key] = entry;
}

void AccHttpCallout::maybeCleanCache() {
	// Evict expired entries at most once every 5 minutes.  m_last_cleanup and
	// the cache are both guarded by m_cache_mutex, so this is safe to call from
	// the many threads that concurrently drive Access().
	std::lock_guard<std::mutex> lock(m_cache_mutex);

	auto now = std::chrono::steady_clock::now();
	if (std::chrono::duration_cast<std::chrono::seconds>(now - m_last_cleanup)
			.count() <= 300) {
		return;
	}
	m_last_cleanup = now;

	for (auto it = m_cache.begin(); it != m_cache.end();) {
		if (now >= it->second.expiration) {
			it = m_cache.erase(it);
		} else {
			++it;
		}
	}

	// Evict expired prefix rules, dropping tokens whose rules have all expired.
	for (auto it = m_prefix_cache.begin(); it != m_prefix_cache.end();) {
		auto &rules = it->second;
		rules.erase(std::remove_if(rules.begin(), rules.end(),
								   [&](const PrefixRule &r) {
									   return now >= r.expiration;
								   }),
					rules.end());
		if (rules.empty()) {
			it = m_prefix_cache.erase(it);
		} else {
			++it;
		}
	}
}
