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

#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucEnv.hh"

class XrdSysError;

class HTTPDirectory : public XrdOssDF {
  public:
	HTTPDirectory(XrdSysError &log) : m_log(log) {}

	virtual ~HTTPDirectory() {}

	virtual int Opendir(const char *path, XrdOucEnv &env) override {
		return -ENOSYS;
	}

	virtual int Readdir(char *buff, int blen) override { return -ENOSYS; }

	virtual int StatRet(struct stat *statStruct) override { return -ENOSYS; }

	virtual int Close(long long *retsz = 0) override { return -ENOSYS; }

  protected:
	XrdSysError &m_log;
};
