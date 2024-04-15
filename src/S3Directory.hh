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

// Leaving in duplicate definitions for now. It remains
// to be seen if we'll need to change these and have specific
// behaviors for either HTTP or S3 variants in the future.

class S3Directory : public HTTPDirectory {
  public:
	S3Directory(XrdSysError &log)
		: HTTPDirectory(log)
	// m_log(log)
	{}

	virtual ~S3Directory() {}

	virtual int Opendir(const char *path, XrdOucEnv &env) override {
		return -ENOSYS;
	}

	int Readdir(char *buff, int blen) override { return -ENOSYS; }

	int StatRet(struct stat *statStruct) override { return -ENOSYS; }

	int Close(long long *retsz = 0) override { return -ENOSYS; }
};
