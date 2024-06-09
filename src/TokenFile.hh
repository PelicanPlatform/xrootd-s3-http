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

#pragma once

#include <XrdSys/XrdSysLogger.hh>
#include <XrdSys/XrdSysPthread.hh>

#include <chrono>
#include <string>

// A class representing a bearer token found from a file on disk
class TokenFile {
  public:
	TokenFile(std::string filename, XrdSysError *log)
		: m_log(log), m_token_file(filename),
		  m_token_mutex(new XrdSysRWLock()) {}

	TokenFile(const TokenFile &) = delete;
	TokenFile(TokenFile &&other) noexcept = default;
	TokenFile &operator=(TokenFile &&other) noexcept = default;

	bool Get(std::string &) const;

  private:
	mutable bool m_token_load_success{false};
	XrdSysError *m_log;
	std::string m_token_file; // Location of a file containing a bearer token
							  // for auth'z.
	mutable std::string m_token_contents; // Cached copy of the token itself.
	mutable std::chrono::steady_clock::time_point
		m_last_token_load; // Last time the token was loaded from disk.
	static const std::chrono::steady_clock::duration m_token_expiry;
	mutable std::unique_ptr<XrdSysRWLock>
		m_token_mutex; // Note: when we move to C++17, convert to
					   // std::shared_mutex
};
