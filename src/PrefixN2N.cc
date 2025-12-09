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

#include "PrefixN2N.hh"

#include <XrdOuc/XrdOucGatherConf.hh>
#include <XrdVersion.hh>

#include <nlohmann/json.hpp>

#include <cstring>
#include <errno.h>
#include <sstream>

using namespace XrdHTTPServer;

XrdVERSIONINFO(XrdOucgetName2Name, PrefixN2N);

extern "C" {
XrdOucName2Name *XrdOucgetName2Name(XrdSysError *eDest, const char *confg,
									const char *parms, const char *lroot,
									const char *rroot) {
	eDest->Say("Copr. 2025 Pelican Project, PrefixN2N plugin v 1.0");

	if (parms) {
		eDest->Say("PrefixN2N: Params: ", parms);
	}
	if (lroot) {
		eDest->Say("PrefixN2N: LocalRoot: ", lroot);
	}

	try {
		return new PrefixN2N(eDest, confg, parms, lroot);
	} catch (const std::exception &e) {
		eDest->Say("PrefixN2N: Failed to initialize: ", e.what());
		return nullptr;
	}
}
}

std::pair<bool, std::string> PrefixN2N::parseJsonString(const char *input) {
	if (!input || *input != '"') {
		return {false, ""};
	}

	// Find the end of the JSON string (closing quote, accounting for escapes)
	const char *p = input + 1;
	while (*p && !(*p == '"' && *(p - 1) != '\\')) {
		// Handle the case where we have \\" (escaped backslash followed by
		// quote)
		if (*p == '\\' && *(p + 1) == '\\') {
			p += 2;
			continue;
		}
		p++;
	}

	if (*p != '"') {
		return {false, ""}; // Unterminated string
	}

	// Extract the JSON string including quotes
	std::string jsonStr(input, p - input + 1);

	try {
		// Use nlohmann::json to parse the string
		auto parsed = nlohmann::json::parse(jsonStr);
		if (!parsed.is_string()) {
			return {false, ""};
		}
		return {true, parsed.get<std::string>()};
	} catch (const nlohmann::json::parse_error &) {
		return {false, ""};
	}
}

bool PrefixN2N::Config(const char *configfn) {
	if (!configfn || !*configfn) {
		return true; // No config file, not an error
	}

	XrdOucGatherConf n2nConf("prefixn2n.rule", m_eDest);
	int result;
	if ((result = n2nConf.Gather(configfn, XrdOucGatherConf::full_lines)) < 0) {
		m_eDest->Say("PrefixN2N: Error ", std::to_string(-result).c_str(),
					 " parsing config file ", configfn);
		return false;
	}

	char *line;
	while ((line = n2nConf.GetLine())) {
		// Skip the "prefixn2n." prefix that GetLine() preserves
		const char *p = line;

		// Skip leading whitespace
		while (*p && (*p == ' ' || *p == '\t')) {
			p++;
		}

		// Check for the directive prefix
		if (std::strncmp(p, "prefixn2n.rule", 14) != 0) {
			continue;
		}
		p += 14;

		// Skip whitespace after directive
		while (*p && (*p == ' ' || *p == '\t')) {
			p++;
		}

		if (!*p) {
			m_eDest->Say("PrefixN2N: prefixn2n.rule requires two arguments: "
						 "matchPrefix substitutePrefix");
			return false;
		}

		// Check for -strict flag
		bool strict = false;
		if (std::strncmp(p, "-strict", 7) == 0 &&
			(p[7] == ' ' || p[7] == '\t' || p[7] == '\0')) {
			strict = true;
			p += 7;
			// Skip whitespace after flag
			while (*p && (*p == ' ' || *p == '\t')) {
				p++;
			}
			if (!*p) {
				m_eDest->Say("PrefixN2N: prefixn2n.rule -strict requires two "
							 "arguments: matchPrefix substitutePrefix");
				return false;
			}
		}

		std::string matchPrefix, substitutePrefix;

		// Parse matchPrefix - check if it's a JSON quoted string
		if (*p == '"') {
			auto [success, parsed] = parseJsonString(p);
			if (!success) {
				m_eDest->Say("PrefixN2N: Failed to parse JSON string for "
							 "matchPrefix in prefixn2n.rule");
				return false;
			}
			matchPrefix = parsed;
			// Skip past the quoted string
			p++; // Skip opening quote
			while (*p && !(*p == '"' && *(p - 1) != '\\')) {
				p++;
			}
			if (*p == '"') {
				p++; // Skip closing quote
			}
		} else {
			// Read until whitespace
			const char *start = p;
			while (*p && *p != ' ' && *p != '\t') {
				p++;
			}
			matchPrefix = std::string(start, p - start);
		}

		// Skip whitespace between arguments
		while (*p && (*p == ' ' || *p == '\t')) {
			p++;
		}

		if (!*p) {
			m_eDest->Say("PrefixN2N: prefixn2n.rule requires two arguments: "
						 "matchPrefix substitutePrefix");
			return false;
		}

		// Parse substitutePrefix
		if (*p == '"') {
			auto [success, parsed] = parseJsonString(p);
			if (!success) {
				m_eDest->Say("PrefixN2N: Failed to parse JSON string for "
							 "substitutePrefix in prefixn2n.rule");
				return false;
			}
			substitutePrefix = parsed;
		} else {
			// Read until whitespace or end of line
			const char *start = p;
			while (*p && *p != ' ' && *p != '\t' && *p != '\n') {
				p++;
			}
			substitutePrefix = std::string(start, p - start);
		}

		addRule(matchPrefix, substitutePrefix, strict);
		m_eDest->Say("PrefixN2N: Added rule from config: ", matchPrefix.c_str(),
					 " -> ", substitutePrefix.c_str(),
					 strict ? " (strict)" : "");
	}

	return true;
}

PrefixN2N::PrefixN2N(XrdSysError *lp, const char *confg, const char *parms,
					 const char *lroot)
	: m_eDest(lp) {
	// Store the local root (with trailing slash stripped if present)
	if (lroot && *lroot) {
		m_localRoot = lroot;
		// Remove trailing slash from localroot (we'll add it as needed)
		while (m_localRoot.size() > 1 && m_localRoot.back() == '/') {
			m_localRoot.pop_back();
		}
	}

	// First, try to parse configuration from the config file
	if (confg && *confg) {
		if (!Config(confg)) {
			throw std::runtime_error("Failed to parse configuration file");
		}
	}

	// Then, parse any parameters passed directly (legacy/override)
	// Parameters are expected as pairs: "matchPrefix1 substPrefix1 matchPrefix2
	// substPrefix2 ..."
	if (parms && *parms) {
		std::istringstream iss(parms);
		std::string matchPrefix, substitutePrefix;
		while (iss >> matchPrefix >> substitutePrefix) {
			addRule(matchPrefix, substitutePrefix);
			m_eDest->Say("PrefixN2N: Added rule from params: ",
						 matchPrefix.c_str(), " -> ", substitutePrefix.c_str());
		}
	}

	if (m_rules.empty()) {
		m_eDest->Say(
			"PrefixN2N: Warning - No rules configured. All paths will pass "
			"through unchanged.");
	}
}

PrefixN2N::~PrefixN2N() {}

std::string PrefixN2N::normalizeForMatch(const std::string &path) {
	if (path.empty()) {
		return "/";
	}

	std::string result = path;

	// Remove trailing slashes (but keep at least one for root)
	while (result.size() > 1 && result.back() == '/') {
		result.pop_back();
	}

	return result;
}

std::string PrefixN2N::normalizeSlashes(const std::string &path) {
	if (path.empty()) {
		return path;
	}

	// Single pass O(n) normalization of consecutive slashes
	std::string result;
	result.reserve(path.size());

	bool lastWasSlash = false;
	for (char c : path) {
		if (c == '/') {
			if (!lastWasSlash) {
				result.push_back(c);
				lastWasSlash = true;
			}
			// Skip additional consecutive slashes
		} else {
			result.push_back(c);
			lastWasSlash = false;
		}
	}

	return result;
}

bool PrefixN2N::pathPrefixMatch(const std::string &path,
								const std::string &prefix) {
	// Normalize both paths for matching (removes trailing slashes)
	std::string normPath = normalizeForMatch(path);
	std::string normPrefix = normalizeForMatch(prefix);

	// Empty prefix matches nothing (except empty path)
	if (normPrefix.empty() || normPrefix == "/") {
		// Root prefix "/" matches everything
		return normPrefix == "/" || normPath.empty();
	}

	// Check if path starts with prefix
	if (normPath.compare(0, normPrefix.size(), normPrefix) != 0) {
		return false;
	}

	// Must be exact match or followed by '/'
	// This ensures /foo matches /foo and /foo/bar but NOT /foobar
	if (normPath.size() == normPrefix.size()) {
		// Exact match
		return true;
	}

	// Path is longer than prefix; check if next char is '/'
	return normPath[normPrefix.size()] == '/';
}

void PrefixN2N::addRule(const std::string &matchPrefix,
						const std::string &substitutePrefix, bool strict) {
	Rule rule;
	rule.matchPrefix = normalizeForMatch(matchPrefix);
	rule.substitutePrefix = normalizeForMatch(substitutePrefix);
	rule.strict = strict;
	m_rules.push_back(rule);
}

int PrefixN2N::applyRules(const char *inputPath, bool forward, char *buff,
						  int blen) {
	if (!inputPath || !buff || blen <= 0) {
		return EINVAL;
	}

	std::string originalPath = inputPath;

	// Check if input has trailing slash (to preserve it later)
	bool hasTrailingSlash = !originalPath.empty() && originalPath.back() == '/';
	// Don't consider root "/" as having a "trailing" slash to preserve
	if (originalPath == "/") {
		hasTrailingSlash = false;
	}

	// Remove trailing slashes for matching (but preserve internal structure)
	std::string pathForMatch = originalPath;
	while (pathForMatch.size() > 1 && pathForMatch.back() == '/') {
		pathForMatch.pop_back();
	}

	// Normalized path for prefix matching (also normalizes double slashes)
	std::string normPath = normalizeForMatch(originalPath);

	// Try each rule in order
	for (const auto &rule : m_rules) {
		const std::string &fromPrefix =
			forward ? rule.matchPrefix : rule.substitutePrefix;
		const std::string &toPrefix =
			forward ? rule.substitutePrefix : rule.matchPrefix;

		if (pathPrefixMatch(normPath, fromPrefix)) {
			// Compute the suffix (part after the matching prefix)
			// In strict mode, use the path that preserves internal //
			// In non-strict mode, use the normalized path
			const std::string &pathForSuffix =
				rule.strict ? pathForMatch : normPath;

			std::string suffix;
			if (pathForSuffix.size() > fromPrefix.size()) {
				suffix = pathForSuffix.substr(fromPrefix.size());
			}

			// Build the result: toPrefix + suffix
			std::string result;
			if (fromPrefix == "/" && !suffix.empty() && suffix[0] != '/') {
				// Root prefix as source: suffix doesn't have leading /, add
				// separator
				result = toPrefix + "/" + suffix;
			} else if (toPrefix == "/" && !suffix.empty() && suffix[0] == '/') {
				// Root prefix as destination: suffix has leading /, don't
				// duplicate
				result = suffix;
			} else {
				result = toPrefix + suffix;
			}

			// If not strict mode, normalize consecutive slashes
			if (!rule.strict) {
				result = normalizeSlashes(result);
			}

			// Preserve trailing slash if input had one
			if (hasTrailingSlash && !result.empty() && result.back() != '/') {
				result.push_back('/');
			}

			// Check buffer size
			if (static_cast<int>(result.size()) >= blen) {
				return ENAMETOOLONG;
			}

			std::strncpy(buff, result.c_str(), blen);
			buff[blen - 1] = '\0'; // Ensure null termination
			return 0;
		}
	}

	// No rule matched; return the path (possibly with slash normalization)
	// For empty path, treat as root "/"
	std::string result = originalPath.empty() ? "/" : originalPath;

	if (static_cast<int>(result.size()) >= blen) {
		return ENAMETOOLONG;
	}

	std::strncpy(buff, result.c_str(), blen);
	buff[blen - 1] = '\0';
	return 0;
}

int PrefixN2N::lfn2pfn(const char *lfn, char *buff, int blen) {
	if (!buff || blen <= 0) {
		return EINVAL;
	}

	// First apply the rules to get the logical->physical transformation
	char tempBuff[4096];
	int rc = applyRules(lfn, true, tempBuff, sizeof(tempBuff));
	if (rc != 0) {
		return rc;
	}

	// Prepend localroot if set
	std::string result;
	if (!m_localRoot.empty()) {
		result = m_localRoot + tempBuff;
	} else {
		result = tempBuff;
	}

	// Check buffer size
	if (static_cast<int>(result.size()) >= blen) {
		return ENAMETOOLONG;
	}

	std::strncpy(buff, result.c_str(), blen);
	buff[blen - 1] = '\0';

	if (m_eDest) {
		std::string msg = std::string("PrefixN2N: lfn2pfn: ") +
						  (lfn ? lfn : "(null)") + " -> " + buff;
		m_eDest->Say(msg.c_str());
	}
	return 0;
}

int PrefixN2N::lfn2rfn(const char *lfn, char *buff, int blen) {
	if (!buff || blen <= 0) {
		return EINVAL;
	}
	// Remote file name uses transformation but WITHOUT localroot
	int rc = applyRules(lfn, true, buff, blen);
	if (m_eDest) {
		std::string msg = std::string("PrefixN2N: lfn2rfn: ") +
						  (lfn ? lfn : "(null)") + " -> " + buff;
		m_eDest->Say(msg.c_str());
	}
	return rc;
}

int PrefixN2N::pfn2lfn(const char *pfn, char *buff, int blen) {
	if (!buff || blen <= 0) {
		return EINVAL;
	}
	// Strip localroot if present before reverse transformation
	const char *pathToTransform = pfn;
	if (pfn && !m_localRoot.empty() &&
		std::strncmp(pfn, m_localRoot.c_str(), m_localRoot.size()) == 0) {
		pathToTransform = pfn + m_localRoot.size();
	}

	int rc = applyRules(pathToTransform, false, buff, blen);
	if (m_eDest) {
		std::string msg = std::string("PrefixN2N: pfn2lfn: ") +
						  (pfn ? pfn : "(null)") + " -> " + buff;
		m_eDest->Say(msg.c_str());
	}
	return rc;
}
