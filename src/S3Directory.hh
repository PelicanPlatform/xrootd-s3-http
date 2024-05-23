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

#include "HTTPDirectory.hh"
#include "S3Commands.hh"
#include "S3FileSystem.hh"

#include <string>
#include <vector>

class XrdSysError;

class S3Directory : public HTTPDirectory {
  public:
	S3Directory(XrdSysError &log, const S3FileSystem &fs)
		: HTTPDirectory(log),
		  m_fs(fs)
	{}

	virtual ~S3Directory() {}

	virtual int Opendir(const char *path, XrdOucEnv &env) override;

	int Readdir(char *buff, int blen) override;

	int StatRet(struct stat *statStruct) override;

	int Close(long long *retsz = 0) override;

  private:
	void Reset();
	int ListS3Dir(const std::string &ct);

	bool m_opened{false};
	ssize_t m_idx{0};
	std::vector<S3ObjectInfo> m_objInfo;
	std::vector<std::string> m_commonPrefixes;
	std::string m_prefix;
	std::string m_ct;
	std::string m_object;
	const S3FileSystem &m_fs;
	S3AccessInfo m_ai;
	struct stat *m_stat_buf{nullptr};
};
