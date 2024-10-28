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

#include "S3FileSystem.hh"

#include <XrdOss/XrdOss.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdSec/XrdSecEntity.hh>
#include <XrdSec/XrdSecEntityAttr.hh>
#include <XrdVersion.hh>

#include <memory>

#include <fcntl.h>

int parse_path(const S3FileSystem &fs, const char *path,
			   std::string &exposedPath, std::string &object);

class AmazonS3SendMultipartPart;

class S3File : public XrdOssDF {
  public:
	S3File(XrdSysError &log, S3FileSystem *oss);

	virtual ~S3File() {}

	int Open(const char *path, int Oflag, mode_t Mode, XrdOucEnv &env) override;

	int Fchmod(mode_t mode) override { return -ENOSYS; }

	void Flush() override {}

	int Fstat(struct stat *buf) override;

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

	ssize_t Read(void *buffer, off_t offset, size_t size) override;

	int Read(XrdSfsAio *aiop) override { return -ENOSYS; }

	ssize_t ReadRaw(void *buffer, off_t offset, size_t size) override {
		return -ENOSYS;
	}

	ssize_t ReadV(XrdOucIOVec *readV, int rdvcnt) override { return -ENOSYS; }

	ssize_t Write(const void *buffer, off_t offset, size_t size) override;

	int Write(XrdSfsAio *aiop) override { return -ENOSYS; }

	ssize_t WriteV(XrdOucIOVec *writeV, int wrvcnt) override { return -ENOSYS; }

	int Close(long long *retsz = 0) override;

	size_t getContentLength() { return content_length; }
	time_t getLastModified() { return last_modified; }

  private:
	ssize_t ContinueSendPart(const void *buffer, size_t size);
	XrdSysError &m_log;
	S3FileSystem *m_oss;

	std::string m_object;
	S3AccessInfo m_ai;

	size_t content_length;
	time_t last_modified;

	static const size_t m_s3_part_size =
		100'000'000; // The size of each S3 chunk.

	bool m_create{false};
	int partNumber;
	size_t m_part_written{
		0}; // Number of bytes written for the current upload chunk.
	size_t m_part_size{0};	 // Size of the current upload chunk (0 if unknon);
	off_t m_write_offset{0}; // Offset of the file pointer for writes (helps
							 // detect out-of-order writes).
	off_t m_object_size{
		-1}; // Expected size of the completed object; -1 if unknown.
	std::string uploadId; // For creates, upload ID as assigned by t
	std::vector<std::string> eTags;
	std::unique_ptr<AmazonS3SendMultipartPart>
		m_write_op; // The in-progress operation for a multi-part upload.
};
