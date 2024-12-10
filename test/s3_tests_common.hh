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

#include "../src/shortfile.hh"

#include <XrdSys/XrdSysLogger.hh>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

class FileSystemFixtureBase : public testing::Test {
  protected:
	FileSystemFixtureBase()
		: m_log(new XrdSysLogger(2, 0)) // Log to stderr, no log rotation
	{}

	void SetUp() override {
		setenv("XRDINSTANCE", "xrootd", 1);
		char tmp_configfn[] = "/tmp/xrootd-s3-gtest.cfg.XXXXXX";
		auto result = mkstemp(tmp_configfn);
		ASSERT_NE(result, -1) << "Failed to create temp file ("
							  << strerror(errno) << ", errno=" << errno << ")";
		m_configfn = std::string(tmp_configfn);

		auto contents = GetConfig();
		ASSERT_FALSE(contents.empty());
		ASSERT_TRUE(writeShortFile(m_configfn, contents, 0))
			<< "Failed to write to temp file (" << strerror(errno)
			<< ", errno=" << errno << ")";
	}

	void TearDown() override {
		if (!m_configfn.empty()) {
			auto rv = unlink(m_configfn.c_str());
			ASSERT_EQ(rv, 0) << "Failed to delete temp file ("
							 << strerror(errno) << ", errno=" << errno << ")";
		}
	}

	virtual std::string GetConfig() = 0;

	std::string m_configfn;
	std::unique_ptr<XrdSysLogger> m_log;
};
