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

#include "HTTPCommands.hh"
#include "HTTPFileSystem.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSec/XrdSecEntityAttr.hh"
#include "XrdVersion.hh"

#include <fcntl.h>
#include <memory>
#include <mutex>

int parse_path(const std::string &hostname, const char *path,
			   std::string &object);

class HTTPFile : public XrdOssDF {
  public:
	HTTPFile(XrdSysError &log, HTTPFileSystem *oss);

	virtual ~HTTPFile() {}

	virtual int Open(const char *path, int Oflag, mode_t Mode,
					 XrdOucEnv &env) override;

	int Fchmod(mode_t mode) override { return -ENOSYS; }

	void Flush() override {}

	virtual int Fstat(struct stat *buf) override;

	int Fsync() override { return -ENOSYS; }

	int Fsync(XrdSfsAio *aiop) override { return -ENOSYS; }

	int Ftruncate(unsigned long long size) override { return -ENOSYS; }

	off_t getMmap(void **addr) override { return 0; }

	int isCompressed(char *cxidp = 0) override { return -ENOSYS; }

	ssize_t pgRead(void *buffer, off_t offset, size_t rdlen, uint32_t *csvec,
				   uint64_t opts) override {
		return -ENOSYS;
	}

	int pgRead(XrdSfsAio *aioparm, uint64_t opts) override { return -ENOSYS; }

	ssize_t pgWrite(void *buffer, off_t offset, size_t wrlen, uint32_t *csvec,
					uint64_t opts) override {
		return -ENOSYS;
	}

	int pgWrite(XrdSfsAio *aioparm, uint64_t opts) override { return -ENOSYS; }

	ssize_t Read(off_t offset, size_t size) override { return -ENOSYS; }

	virtual ssize_t Read(void *buffer, off_t offset, size_t size) override;

	int Read(XrdSfsAio *aiop) override { return -ENOSYS; }

	ssize_t ReadRaw(void *buffer, off_t offset, size_t size) override {
		return -ENOSYS;
	}

	ssize_t ReadV(XrdOucIOVec *readV, int rdvcnt) override { return -ENOSYS; }

	virtual ssize_t Write(const void *buffer, off_t offset,
						  size_t size) override;

	int Write(XrdSfsAio *aiop) override { return -ENOSYS; }

	ssize_t WriteV(XrdOucIOVec *writeV, int wrvcnt) override { return -ENOSYS; }

	virtual int
	Close(long long *retsz = 0) override; // upstream is abstract definition

	size_t getContentLength() { return content_length; }
	time_t getLastModified() { return last_modified; }

  private:
	bool m_stat{false};

	XrdSysError &m_log;
	HTTPFileSystem *m_oss;

	std::string m_hostname;
	std::string m_hostUrl;
	std::string m_object;
	// Whether the file was opened in write mode
	bool m_write{false};
	// Whether the file is open
	bool m_is_open{false};
	// Expected size of the completed object; -1 if unknown.
	off_t m_object_size{-1};
	off_t m_write_offset{0};
	std::unique_ptr<std::mutex> m_write_lk;
	// The in-progress operation for a multi-part upload; its lifetime may be
	// spread across multiple write calls.
	std::unique_ptr<HTTPUpload> m_write_op;

	size_t content_length;
	time_t last_modified;
};
