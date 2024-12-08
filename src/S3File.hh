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
#include <mutex>
#include <thread>

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

	// Launch the global monitor thread associated with S3File objects.
	// Currently, the monitor thread is used to cleanup in-progress file
	// transfers that have been abandoned.
	static void LaunchMonitorThread();

  private:
	// Periodic cleanup of in-progress transfers.
	//
	// Iterates through the global list of pending multipart uploads
	// that may be paused.  For each, call `Tick` on the upload and
	// see if the transfer has aborted.
	static void CleanupTransfers();

	// Single cleanup run for in-progress transfers.
	static void CleanupTransfersOnce();

	// Write data while in "streaming mode" where we don't know the
	// ultimate size of the file (and hence can't start streaming
	// partitions immediately).
	ssize_t WriteStreaming(const void *buffer, off_t offset, size_t size);

	// Send a fully-buffered part of the file; only used while in
	// "streaming" mode.
	ssize_t SendPartStreaming();

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
	// When using the "streaming mode", the upload part has to be completely
	// buffered within the S3File object; this is the current buffer.
	std::string m_streaming_buffer;

	// The mutex protecting write activities.  Writes must currently be
	// serialized as we aggregate them into large operations and upload them to
	// the S3 endpoint. The mutex prevents corruption of internal state.
	//
	// The periodic cleanup thread may decide to abort the in-progress transfer;
	// to do so, it'll need a reference to this lock that is independent of the
	// lifetime of the open file; hence, it's a shared pointer.
	std::shared_ptr<std::mutex> m_write_lk;

	// The in-progress operation for a multi-part upload; its lifetime may be
	// spread across multiple write calls.
	std::shared_ptr<AmazonS3SendMultipartPart>
		m_write_op; // The in-progress operation for a multi-part upload.

	// The multipart uploads represent an in-progress request and the global
	// cleanup thread may decide to trigger a failure if the request does not
	// advance after some time period.
	//
	// To do so, we must be able to lock the associated write mutex and then
	// call `Tick` on the upload.  To avoid prolonging the lifetime of the
	// objects beyond the S3File, we hold onto a reference via a weak pointer.
	// Mutable operations on this vector are protected by the `m_pending_lk`.
	static std::vector<std::pair<std::weak_ptr<std::mutex>,
								 std::weak_ptr<AmazonS3SendMultipartPart>>>
		m_pending_ops;

	// Mutex protecting the m_pending_ops variable.
	static std::mutex m_pending_lk;

	// Flag determining whether the monitoring thread has been launched.
	static std::once_flag m_monitor_launch;
};
