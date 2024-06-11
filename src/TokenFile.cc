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

#include "TokenFile.hh"
#include "logging.hh"
#include "shortfile.hh"
#include "stl_string_utils.hh"

#include <sstream>

using namespace std::chrono_literals;

const std::chrono::steady_clock::duration TokenFile::m_token_expiry = 5s;

// Retrieve the bearer token to use with HTTP requests
//
// Returns true on success and sets `token` to the value of
// the bearer token to use.  If there were no errors - but no
// token is to be used - token is set to the empty string.
// Otherwise, returns false.
bool TokenFile::Get(std::string &token) const {
	if (m_token_file.empty()) {
		token.clear();
		return true;
	}

	XrdSysRWLockHelper lock(m_token_mutex.get(), true);
	if (m_token_load_success) {
		auto now = std::chrono::steady_clock::now();
		if (now - m_last_token_load <= m_token_expiry) {
			token = m_token_contents;
			return true;
		}
	}
	lock.UnLock();

	// Upgrade to write lock - we will mutate the data structures.
	lock.Lock(m_token_mutex.get(), false);
	std::string contents;
	if (!readShortFile(m_token_file, contents)) {
		if (m_log) {
			m_log->Log(
				XrdHTTPServer::LogMask::Warning, "getAuthToken",
				"Failed to read token authorization file:", strerror(errno));
		}
		m_token_load_success = false;
		return false;
	}
	std::istringstream istream;
	istream.str(contents);
	m_last_token_load = std::chrono::steady_clock::now();
	m_token_load_success = true;
	for (std::string line; std::getline(istream, line);) {
		trim(line);
		if (line.empty()) {
			continue;
		}
		if (line[0] == '#') {
			continue;
		}
		m_token_contents = line;
		token = m_token_contents;
		return true;
	}
	// If there are no error reading the file but the file has no tokens, we
	// assume this indicates no token should be used.
	token = "";
	return true;
}
