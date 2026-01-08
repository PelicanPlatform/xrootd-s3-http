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

#include <XrdOuc/XrdOucGatherConf.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdVersion.hh>

#include <nlohmann/json.hpp>

#include <curl/curl.h>

#include <cctype>
#include <cstring>
#include <errno.h>
#include <iomanip>
#include <openssl/sha.h>
#include <sstream>

using namespace XrdHTTPServer;

XrdVERSIONINFO(XrdAccAuthorizeObject, AccHttpCallout);

extern "C" {
XrdAccAuthorize *XrdAccAuthorizeObject(XrdSysLogger *lp, const char *cfn,
									   const char *parm) {
	XrdSysError eDest(lp, "acchttpcallout");
	eDest.Say("Copr. 2025 Pelican Project, AccHttpCallout plugin v 1.0");

	if (parm) {
		eDest.Say("AccHttpCallout: Params: ", parm);
	}

	try {
		return new AccHttpCallout(&eDest, cfn, parm);
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
							   const char *parms)
	: m_eDest(lp), m_last_cleanup(std::chrono::steady_clock::now()) {
	if (confg && !Config(confg)) {
		throw std::runtime_error("Failed to configure AccHttpCallout");
	}

	if (m_endpoint.empty()) {
		throw std::runtime_error(
			"AccHttpCallout: acchttpcallout.endpoint must be configured");
	}

	// Initialize curl globally
	curl_global_init(CURL_GLOBAL_ALL);
}

AccHttpCallout::~AccHttpCallout() { curl_global_cleanup(); }

bool AccHttpCallout::Config(const char *configfn) {
	XrdOucGatherConf conf("acchttpcallout.", m_eDest);
	if (conf.Gather(configfn, XrdOucGatherConf::full_lines) < 0) {
		m_eDest->Say("AccHttpCallout: Failed to gather configuration");
		return false;
	}

	const auto &lines = conf.GetLines();
	for (const auto &line : lines) {
		std::istringstream iss(line);
		std::string directive;
		iss >> directive;

		if (directive == "acchttpcallout.endpoint") {
			iss >> m_endpoint;
			m_eDest->Say("AccHttpCallout: Endpoint set to: ",
						 m_endpoint.c_str());
		} else if (directive == "acchttpcallout.cache_ttl_positive") {
			iss >> m_cache_ttl_positive;
			m_eDest->Say("AccHttpCallout: Positive cache TTL set to: ",
						 std::to_string(m_cache_ttl_positive).c_str(),
						 " seconds");
		} else if (directive == "acchttpcallout.cache_ttl_negative") {
			iss >> m_cache_ttl_negative;
			m_eDest->Say("AccHttpCallout: Negative cache TTL set to: ",
						 std::to_string(m_cache_ttl_negative).c_str(),
						 " seconds");
		} else if (directive == "acchttpcallout.passthrough") {
			std::string value;
			iss >> value;
			m_passthrough = (value == "true" || value == "1");
			m_eDest->Say("AccHttpCallout: Passthrough set to: ",
						 m_passthrough ? "true" : "false");
		} else if (directive == "acchttpcallout.trace") {
			// Handle trace directive if needed
			std::string level;
			iss >> level;
			m_eDest->Say("AccHttpCallout: Trace level: ", level.c_str());
		}
	}

	return true;
}

XrdAccPrivs AccHttpCallout::Access(const XrdSecEntity *Entity,
								   const char *path,
								   const Access_Operation oper,
								   XrdOucEnv *Env) {
	std::string eInfo;
	return Access(Entity, path, oper, eInfo, Env);
}

XrdAccPrivs AccHttpCallout::Access(const XrdSecEntity *Entity,
								   const char *path,
								   const Access_Operation oper,
								   std::string &eInfo, XrdOucEnv *Env) {
	// Get the bearer token from the entity
	std::string token;
	if (Entity && Entity->endorsements) {
		token = Entity->endorsements;
	}

	if (token.empty()) {
		eInfo = "No bearer token provided";
		m_eDest->Say("AccHttpCallout: No bearer token for path: ", path);
		// Note: The passthrough configuration is a deployment hint.
		// We return XrdAccPriv_None here, and XRootD's framework will
		// try the next plugin in the chain if one is configured.
		return XrdAccPrivs(XrdAccPriv_None);
	}

	// Convert operation to verb
	std::string verb = operationToVerb(oper);

	// Generate cache key
	std::string cacheKey = generateCacheKey(token, path, oper);

	// Check cache first
	CacheEntry entry;
	if (lookupCache(cacheKey, entry)) {
		m_eDest->Say("AccHttpCallout: Cache hit for path: ", path);
		return entry.privileges;
	}

	// Make HTTP callout
	std::vector<AuthInfo> authInfos;
	std::string userInfo, groupInfo;
	int statusCode =
		makeHttpCallout(token, path, verb, eInfo, authInfos, userInfo, groupInfo);

	XrdAccPrivs privileges;
	int ttl;

	if (statusCode == 200) {
		// Authorized
		privileges = XrdAccPrivs(~0); // All privileges
		ttl = m_cache_ttl_positive;

		// Cache additional authorizations from response
		for (const auto &authInfo : authInfos) {
			for (const auto &prefix : authInfo.prefixes) {
				Access_Operation op = verbToOperation(authInfo.verb);
				std::string prefixKey = generateCacheKey(token, prefix, op);
				storeCache(prefixKey, authInfo.privileges, ttl, userInfo,
						   groupInfo);
			}
		}
	} else if (statusCode == 401 || statusCode == 403) {
		// Denied
		privileges = XrdAccPrivs(XrdAccPriv_None);
		ttl = m_cache_ttl_negative;
	} else {
		// Error - authorization service is not responding correctly
		eInfo = "Authorization service error: " + std::to_string(statusCode);
		m_eDest->Say("AccHttpCallout: HTTP error ", std::to_string(statusCode).c_str(), " for path: ", path);
		// Note: The passthrough configuration is a deployment hint about
		// how this plugin is used in the authorization chain. When the
		// authorization service fails, we return XrdAccPriv_None. XRootD's
		// framework will try the next plugin if one is configured, or deny
		// access if this is the only/last plugin in the chain.
		return XrdAccPrivs(XrdAccPriv_None);
	}

	// Store in cache
	storeCache(cacheKey, privileges, ttl, userInfo, groupInfo);

	// Periodically clean cache
	auto now = std::chrono::steady_clock::now();
	if (std::chrono::duration_cast<std::chrono::seconds>(now - m_last_cleanup)
			.count() > 300) {
		cleanCache();
		m_last_cleanup = now;
	}

	return privileges;
}

int AccHttpCallout::Audit(const int accok, const XrdSecEntity *Entity,
						  const char *path, const Access_Operation oper,
						  XrdOucEnv *Env) {
	// Simple audit logging
	const char *result = accok ? "GRANTED" : "DENIED";
	const char *user = Entity && Entity->name ? Entity->name : "unknown";
	std::string verb = operationToVerb(oper);

	m_eDest->Say("AccHttpCallout: Audit: ", result, " user=", user, " path=",
				 path, " verb=", verb.c_str());

	return 1;
}

int AccHttpCallout::Test(const XrdAccPrivs priv,
						 const Access_Operation oper) {
	// Simple test: if any privileges are set, allow the operation
	// A more sophisticated implementation would check specific privileges
	return priv != XrdAccPriv_None;
}

int AccHttpCallout::makeHttpCallout(const std::string &token,
									const std::string &path,
									const std::string &verb,
									std::string &eInfo,
									std::vector<AuthInfo> &authInfos,
									std::string &userInfo,
									std::string &groupInfo) {
	CURL *curl = curl_easy_init();
	if (!curl) {
		eInfo = "Failed to initialize CURL";
		return 500;
	}

	// Build URL with query parameters
	std::string url = m_endpoint + "?path=" + urlEncode(path) +
					  "&verb=" + urlEncode(verb);

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
							info.prefixes.push_back(
								prefix.get<std::string>());
						}
					}
					info.privileges = XrdAccPrivs(~0); // All privileges
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

std::string
AccHttpCallout::operationToVerb(const Access_Operation oper) {
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

Access_Operation
AccHttpCallout::verbToOperation(const std::string &verb) {
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
	entry.expiration = std::chrono::steady_clock::now() +
					   std::chrono::seconds(ttl);
	entry.userInfo = userInfo;
	entry.groupInfo = groupInfo;

	m_cache[key] = entry;
}

void AccHttpCallout::cleanCache() {
	std::lock_guard<std::mutex> lock(m_cache_mutex);

	auto now = std::chrono::steady_clock::now();
	for (auto it = m_cache.begin(); it != m_cache.end();) {
		if (now >= it->second.expiration) {
			it = m_cache.erase(it);
		} else {
			++it;
		}
	}
}
