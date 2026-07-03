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
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace XrdHTTPServer {

/**
 * Authorization plugin that makes HTTP callouts to determine access.
 *
 * This plugin uses HTTP GET requests to an external authorization service to
 * determine whether a client should be granted access to a resource.  The
 * client's bearer token is passed in the Authorization header and the path and
 * operation are passed as query parameters.  It can optionally wrap another
 * authorization object and delegate to it when it cannot grant access itself
 * (see httpcallout.passthrough).
 *
 * Configuration directives:
 *   httpcallout.endpoint <url>               - The HTTP(S) endpoint to call
 *   httpcallout.cache_ttl_positive <seconds> - Cache time for grants (60)
 *   httpcallout.cache_ttl_negative <seconds> - Cache time for denials (30)
 *   httpcallout.passthrough [true|false]     - When true, delegate to the
 *                                                 chained authorization object
 *                                                 on any non-grant (default
 *                                                 false: deny)
 *   httpcallout.trace [all|error|warning|info|debug|none] - Logging level
 */
class AccHttpCallout : public XrdAccAuthorize {
  public:
	/**
	 * Construct an AccHttpCallout instance.
	 *
	 * @param lp     Error logger
	 * @param confg  Path to configuration file
	 * @param parms  Configuration parameters string
	 * @param chain  The previously-configured authorization object to delegate
	 *               to when passthrough is enabled (may be null)
	 */
	AccHttpCallout(XrdSysError *lp, const char *confg, const char *parms,
				   XrdAccAuthorize *chain = nullptr);

	virtual ~AccHttpCallout();

	XrdAccPrivs Access(const XrdSecEntity *Entity, const char *path,
					   const Access_Operation oper,
					   XrdOucEnv *Env = 0) override;

	// Extended variant that also returns human-readable error info.  This is
	// not part of the XrdAccAuthorize interface (hence not an override); it is
	// a helper that the interface Access() above delegates to.
	XrdAccPrivs Access(const XrdSecEntity *Entity, const char *path,
					   const Access_Operation oper, std::string &eInfo,
					   XrdOucEnv *Env = 0);

	int Audit(const int accok, const XrdSecEntity *Entity, const char *path,
			  const Access_Operation oper, XrdOucEnv *Env = 0) override;

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
	 * A cached authorization decision for an exact (token, path, op) triple.
	 * `privileges == XrdAccPriv_None` records a cached non-grant.
	 */
	struct CacheEntry {
		XrdAccPrivs privileges;
		std::chrono::steady_clock::time_point expiration;
		std::string userInfo;
		std::string groupInfo;
	};

	/**
	 * A path-prefix authorization returned in a response body: the token is
	 * authorized for `oper` on any path at or below `prefix` until expiration.
	 */
	struct PrefixRule {
		Access_Operation oper;
		std::string prefix;
		std::chrono::steady_clock::time_point expiration;
	};

	/**
	 * Additional authorization info parsed from a response body.
	 */
	struct AuthInfo {
		std::vector<std::string> prefixes; // Path prefixes authorized
		std::string verb;				   // HTTP/WebDAV verb
	};

	// Extract the client's bearer token, mirroring how the SciTokens plugin
	// locates it: the `authz` CGI value (with any "Bearer " prefix stripped),
	// then the security entity's credentials.  Returns "" if none is found.
	std::string extractToken(const XrdSecEntity *Entity, XrdOucEnv *Env) const;

	// Result when this plugin cannot grant access itself: delegate to the
	// chained authorization object if passthrough is enabled, else deny.
	XrdAccPrivs onFailure(const XrdSecEntity *Entity, const char *path,
						  const Access_Operation oper, XrdOucEnv *Env);

	int makeHttpCallout(const std::string &token, const std::string &path,
						const std::string &verb, std::string &eInfo,
						std::vector<AuthInfo> &authInfos, std::string &userInfo,
						std::string &groupInfo);

	static std::string operationToVerb(const Access_Operation oper);
	static Access_Operation verbToOperation(const std::string &verb);

	// SHA256-based keys: the full (token,path,op) triple for the exact cache,
	// and just the token for the per-token prefix-rule table.
	static std::string generateCacheKey(const std::string &token,
										const std::string &path,
										const Access_Operation oper);
	static std::string hashToken(const std::string &token);

	// True if `path` is at or below `prefix` (component-aware, so "/a/b" is a
	// prefix of "/a/b" and "/a/b/c" but not "/a/bc").
	static bool pathHasPrefix(const std::string &path,
							  const std::string &prefix);

	bool lookupCache(const std::string &key, CacheEntry &entry);
	void storeCache(const std::string &key, const XrdAccPrivs privileges,
					int ttl, const std::string &userInfo,
					const std::string &groupInfo);

	// Grant from a cached path-prefix rule, if one covers (tokenKey, path,
	// oper).  Returns true on a cache hit.
	bool lookupPrefixCache(const std::string &tokenKey, const std::string &path,
						   const Access_Operation oper);
	// Record the path-prefix authorizations from a response body.
	void storePrefixRules(const std::string &tokenKey,
						  const std::vector<AuthInfo> &authInfos, int ttl);

	/**
	 * Evict expired entries from the caches, but at most once every few
	 * minutes.  Guards m_last_cleanup and the caches under m_cache_mutex.
	 */
	void maybeCleanCache();

	std::string m_endpoint;		  // HTTP(S) endpoint URL
	int m_cache_ttl_positive{60}; // Cache TTL for positive responses (seconds)
	int m_cache_ttl_negative{30}; // Cache TTL for negative responses (seconds)
	bool m_passthrough{false};	  // Delegate to the chain on non-grant

	// The next authorization object in the chain (from XrdAccAuthorizeObjAdd).
	XrdAccAuthorize *m_chain{nullptr};

	std::unordered_map<std::string, CacheEntry> m_cache;
	std::unordered_map<std::string, std::vector<PrefixRule>> m_prefix_cache;
	std::mutex m_cache_mutex;
	std::chrono::steady_clock::time_point
		m_last_cleanup; // Last time the caches were cleaned

	XrdSysError *m_eDest;
};

} // namespace XrdHTTPServer

extern "C" {
XrdAccAuthorize *XrdAccAuthorizeObjAdd(XrdSysLogger *lp, const char *cfn,
									   const char *parm, XrdOucEnv *envP,
									   XrdAccAuthorize *accP);
}

#endif // ACCHTTPCALLOUT_HH
