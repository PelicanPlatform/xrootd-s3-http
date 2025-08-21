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

#include "HTTPFileSystem.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "logging.hh"
#include <XrdSfs/XrdSfsInterface.hh>
#include <map>

using namespace XrdHTTPServer;

class HTTPDirectory : public XrdOssDF {
  public:
	HTTPDirectory(XrdSysError &log, HTTPFileSystem *oss);
	virtual ~HTTPDirectory() {}

	virtual int Opendir(const char *path, XrdOucEnv &env) override;

	virtual int Readdir(char *buff, int blen) override;

	virtual int StatRet(struct stat *statStruct) override {
		mystat = statStruct;
		return SFS_OK;
	}

	virtual int Close(long long *retsz = 0) override { return -ENOSYS; }

  protected:
	struct FSSpecEntry {
		std::string mode;
		std::string flags;
		std::string size;
		std::string modified;
		std::string name;
	};

	std::map<std::string, struct stat>
	parseHTMLToFSSpecString(const std::string &htmlContent);
	std::map<std::string, struct stat>
	parseWebDAVToFSSpecString(const std::string &htmlContent);
	std::string extractHTMLTable(const std::string &htmlContent);

	struct stat *mystat;
	XrdSysError &m_log;
	std::string m_object;
	HTTPFileSystem *m_oss;
	std::string m_hostname;
	std::string m_hostUrl;
	std::map<std::string, struct stat> m_remoteList;
	std::string m_remote_flavor;
	int m_bytesReturned;
};
