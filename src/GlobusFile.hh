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

#pragma once

#include "GlobusFileSystem.hh"
#include <XrdOss/XrdOssWrapper.hh>

class GlobusFile final : public XrdOssWrapDF {
  public:
	GlobusFile(std::unique_ptr<XrdOssDF> wrapped, XrdSysError &log)
		: XrdOssWrapDF(*wrapped), m_wrapped(std::move(wrapped)) {}

	virtual ~GlobusFile() {}

	int Open(const char *path, int Oflag, mode_t Mode,
			 XrdOucEnv &env) override {
		return m_wrapped->Open(path, Oflag, Mode, env);
	}
	int Fstat(struct stat *buf) override { return m_wrapped->Fstat(buf); }
	ssize_t Read(void *buffer, off_t offset, size_t size) override {
		return m_wrapped->Read(buffer, offset, size);
	}
	ssize_t Write(const void *buffer, off_t offset, size_t size) override {
		return m_wrapped->Write(buffer, offset, size);
	}
	int Close(long long *retsz = 0) override { return m_wrapped->Close(retsz); }

  private:
	std::unique_ptr<XrdOssDF> m_wrapped;
};
