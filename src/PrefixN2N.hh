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

#ifndef PREFIXN2N_HH
#define PREFIXN2N_HH

#include <XrdOuc/XrdOucName2Name.hh>
#include <XrdSys/XrdSysError.hh>

#include <string>
#include <utility>
#include <vector>

namespace XrdHTTPServer {

/**
 * A simplified Name2Name module that performs path prefix substitution.
 *
 * Rules consist of a matching prefix and a substitution prefix.
 * If a logical path matches a prefix at a path boundary (not substring),
 * the matching prefix is stripped and replaced with the substitution prefix.
 *
 * Path boundary matching means:
 *   - /foo matches /foo and /foo/bar but NOT /foobar
 *   - The match must occur at a '/' boundary or be an exact match
 *
 * Configuration is done via the config file with directives like:
 *   prefixn2n.rule /source/prefix /destination/prefix
 *   prefixn2n.rule -strict /source /dest   # strict mode: preserve // exactly
 *
 * For paths containing spaces, use JSON-style quoted strings:
 *   prefixn2n.rule "/path with spaces" "/destination with spaces"
 *
 * Trailing slashes in input paths are preserved in output paths.
 *
 * By default, multiple consecutive slashes (//) are normalized to single
 * slashes. Use -strict flag to preserve // exactly.
 */
class PrefixN2N : public XrdOucName2Name {
  public:
	/**
	 * Represents a single prefix substitution rule.
	 */
	struct Rule {
		std::string matchPrefix; // Prefix to match (e.g., "/store")
		std::string
			substitutePrefix; // Prefix to substitute (e.g., "/data/cms")
		bool strict =
			false; // If true, preserve // exactly; if false, normalize to /
	};

	/**
	 * Construct a PrefixN2N instance.
	 *
	 * @param lp     Error logger
	 * @param confg  Path to configuration file
	 * @param parms  Configuration parameters string (legacy)
	 * @param lroot  Local root path (prepended to physical paths)
	 */
	PrefixN2N(XrdSysError *lp, const char *confg, const char *parms,
			  const char *lroot);

	virtual ~PrefixN2N();

	/**
	 * Map a logical file name to a physical file name.
	 *
	 * @param lfn   Logical file name
	 * @param buff  Buffer to store the physical file name
	 * @param blen  Length of the buffer
	 * @return 0 on success, errno on failure
	 */
	int lfn2pfn(const char *lfn, char *buff, int blen) override;

	/**
	 * Map a logical file name to a remote file name.
	 * For this implementation, this is the same as lfn2pfn.
	 *
	 * @param lfn   Logical file name
	 * @param buff  Buffer to store the remote file name
	 * @param blen  Length of the buffer
	 * @return 0 on success, errno on failure
	 */
	int lfn2rfn(const char *lfn, char *buff, int blen) override;

	/**
	 * Map a physical file name to a logical file name.
	 *
	 * @param pfn   Physical file name
	 * @param buff  Buffer to store the logical file name
	 * @param blen  Length of the buffer
	 * @return 0 on success, errno on failure
	 */
	int pfn2lfn(const char *pfn, char *buff, int blen) override;

	/**
	 * Add a prefix substitution rule.
	 *
	 * @param matchPrefix       The prefix to match
	 * @param substitutePrefix  The prefix to substitute
	 * @param strict            If true, preserve // exactly; if false
	 * (default), normalize to /
	 */
	void addRule(const std::string &matchPrefix,
				 const std::string &substitutePrefix, bool strict = false);

	/**
	 * Get the current rules (for testing).
	 *
	 * @return Reference to the rules vector
	 */
	const std::vector<Rule> &getRules() const { return m_rules; }

	/**
	 * Parse a JSON-style quoted string.
	 * Handles escape sequences like \", \\, \n, \t, etc.
	 *
	 * @param input  The input string starting with "
	 * @return A pair of (success, parsed_string)
	 */
	static std::pair<bool, std::string> parseJsonString(const char *input);

	/**
	 * Parse configuration from a file.
	 *
	 * @param configfn Path to the configuration file
	 * @return true on success, false on failure
	 */
	bool Config(const char *configfn);

  private:
	/**
	 * Check if a path matches a prefix at a path boundary.
	 *
	 * @param path   The path to check
	 * @param prefix The prefix to match
	 * @return true if path matches prefix at a path boundary
	 */
	static bool pathPrefixMatch(const std::string &path,
								const std::string &prefix);

	/**
	 * Apply rules to transform a path in the given direction.
	 *
	 * @param inputPath The path to transform
	 * @param forward   If true, apply match->substitute; if false, reverse
	 * @param buff      Buffer to store the result
	 * @param blen      Length of the buffer
	 * @return 0 on success, errno on failure
	 */
	int applyRules(const char *inputPath, bool forward, char *buff, int blen);

	/**
	 * Normalize a path for prefix matching (removes trailing slashes except for
	 * root).
	 *
	 * @param path The path to normalize
	 * @return Normalized path (without trailing slash unless root)
	 */
	static std::string normalizeForMatch(const std::string &path);

	/**
	 * Normalize consecutive slashes in a path (O(n) complexity).
	 * Converts multiple consecutive slashes to single slashes.
	 *
	 * @param path The path to normalize
	 * @return Path with consecutive slashes normalized
	 */
	static std::string normalizeSlashes(const std::string &path);

	std::vector<Rule> m_rules;
	std::string m_localRoot; // Local root prefix to prepend to physical paths
	XrdSysError *m_eDest;
};

} // namespace XrdHTTPServer

extern "C" {
XrdOucName2Name *XrdOucgetName2Name(XrdOucgetName2NameArgs);
}

#endif // PREFIXN2N_HH
