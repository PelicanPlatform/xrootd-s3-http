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

#ifndef ACCHTTPCALLOUT_HH
#define ACCHTTPCALLOUT_HH

#include <XrdAcc/XrdAccAuthorize.hh>
#include <XrdSys/XrdSysError.hh>

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace XrdHTTPServer {

/**
 * Authorization plugin that makes HTTP callouts to determine access.
 *
 * This plugin uses HTTP GET requests to an external authorization service
 * to determine whether a client should be granted access to a resource.
 * The token is passed as a bearer token in the Authorization header, and
 * the path and operation are passed as query parameters.
 *
 * Configuration directives:
 *   acchttpcallout.endpoint <url>        - The HTTP(S) endpoint to call
 *   acchttpcallout.cache_ttl_positive <seconds> - Cache time for positive responses (default: 60)
 *   acchttpcallout.cache_ttl_negative <seconds> - Cache time for negative responses (default: 30)
 *   acchttpcallout.passthrough [true|false] - Pass to next plugin on failure (default: false)
 *   acchttpcallout.trace [all|error|warning|info|debug|none] - Logging level
 */
class AccHttpCallout : public XrdAccAuthorize {
  public:
	/**
	 * Construct an AccHttpCallout instance.
	 *
	 * @param lp     Error logger
	 * @param confg  Path to configuration file
	 * @param parms  Configuration parameters string
	 */
	AccHttpCallout(XrdSysError *lp, const char *confg, const char *parms);

	virtual ~AccHttpCallout();

	/**
	 * Check whether or not the client is permitted specified access to a path.
	 *
	 * @param Entity    -> Authentication information
	 * @param path      -> The logical path which is the target of oper
	 * @param oper      -> The operation being attempted
	 * @param Env       -> Environmental information (optional)
	 * @return Permit: non-zero value; Deny: zero
	 */
	XrdAccPrivs Access(const XrdSecEntity *Entity, const char *path,
					   const Access_Operation oper,
					   XrdOucEnv *Env = 0) override;

	/**
	 * Check whether or not the client is permitted specified access to a path.
	 * Version 2 with extended error information.
	 *
	 * @param Entity    -> Authentication information
	 * @param path      -> The logical path which is the target of oper
	 * @param oper      -> The operation being attempted
	 * @param eInfo     -> Reference to string for extended error info
	 * @param Env       -> Environmental information (optional)
	 * @return Permit: non-zero value; Deny: zero
	 */
	XrdAccPrivs Access(const XrdSecEntity *Entity, const char *path,
					   const Access_Operation oper, std::string &eInfo,
					   XrdOucEnv *Env = 0) override;

	/**
	 * Route an audit message to the appropriate audit exit routine.
	 *
	 * @param accok     -> True if access was granted; false otherwise
	 * @param Entity    -> Authentication information
	 * @param path      -> The logical path which is the target of oper
	 * @param oper      -> The operation being attempted
	 * @param Env       -> Environmental information (optional)
	 * @return Success: !0; Failure: 0
	 */
	int Audit(const int accok, const XrdSecEntity *Entity, const char *path,
			  const Access_Operation oper, XrdOucEnv *Env = 0) override;

	/**
	 * Check whether the specified operation is permitted.
	 *
	 * @param priv      -> The privileges as returned by Access()
	 * @param oper      -> The operation being attempted
	 * @return Permit: non-zero value; Deny: zero
	 */
	int Test(const XrdAccPrivs priv, const Access_Operation oper) override;

	/**
	 * Parse configuration from a file.
	 *
	 * @param configfn Path to the configuration file
	 * @return true on success, false on failure
	 */
	bool Config(const char *configfn);

  private:
	/**
	 * Represents a cached authorization decision.
	 */
	struct CacheEntry {
		XrdAccPrivs privileges;
		std::chrono::steady_clock::time_point expiration;
		std::string userInfo;
		std::string groupInfo;
	};

	/**
	 * Represents additional authorization info from response.
	 */
	struct AuthInfo {
		std::vector<std::string> prefixes; // Path prefixes authorized
		std::string verb; // HTTP/WebDAV verb
		XrdAccPrivs privileges;
	};

	/**
	 * Make an HTTP callout to determine authorization.
	 *
	 * @param token     The bearer token to pass
	 * @param path      The path being accessed
	 * @param verb      The HTTP/WebDAV verb
	 * @param eInfo     Extended error information
	 * @param authInfos Vector to populate with additional authorizations
	 * @param userInfo  String to populate with user info
	 * @param groupInfo String to populate with group info
	 * @return HTTP status code
	 */
	int makeHttpCallout(const std::string &token, const std::string &path,
						const std::string &verb, std::string &eInfo,
						std::vector<AuthInfo> &authInfos,
						std::string &userInfo, std::string &groupInfo);

	/**
	 * Convert Access_Operation to HTTP/WebDAV verb.
	 *
	 * @param oper The operation
	 * @return The corresponding verb
	 */
	static std::string operationToVerb(const Access_Operation oper);

	/**
	 * Convert HTTP/WebDAV verb to Access_Operation.
	 *
	 * @param verb The verb
	 * @return The corresponding operation
	 */
	static Access_Operation verbToOperation(const std::string &verb);

	/**
	 * Generate cache key from token, path, and operation.
	 *
	 * @param token The bearer token
	 * @param path  The path
	 * @param oper  The operation
	 * @return The cache key
	 */
	static std::string generateCacheKey(const std::string &token,
										 const std::string &path,
										 const Access_Operation oper);

	/**
	 * Lookup authorization in cache.
	 *
	 * @param key   The cache key
	 * @param entry Reference to populate with cache entry if found
	 * @return true if found and not expired, false otherwise
	 */
	bool lookupCache(const std::string &key, CacheEntry &entry);

	/**
	 * Store authorization in cache.
	 *
	 * @param key        The cache key
	 * @param privileges The privileges to cache
	 * @param ttl        Time-to-live in seconds
	 * @param userInfo   User information
	 * @param groupInfo  Group information
	 */
	void storeCache(const std::string &key, const XrdAccPrivs privileges,
					int ttl, const std::string &userInfo,
					const std::string &groupInfo);

	/**
	 * Clean expired entries from cache.
	 */
	void cleanCache();

	std::string m_endpoint; // HTTP(S) endpoint URL
	int m_cache_ttl_positive{60}; // Cache TTL for positive responses (seconds)
	int m_cache_ttl_negative{30}; // Cache TTL for negative responses (seconds)
	bool m_passthrough{false}; // Pass through to next plugin on failure

	std::unordered_map<std::string, CacheEntry> m_cache;
	std::mutex m_cache_mutex;
	std::chrono::steady_clock::time_point
		m_last_cleanup; // Last time cache was cleaned

	XrdSysError *m_eDest;
};

} // namespace XrdHTTPServer

extern "C" {
XrdAccAuthorize *XrdAccAuthorizeObject(XrdSysLogger *lp, const char *cfn,
									   const char *parm);
}

#endif // ACCHTTPCALLOUT_HH
