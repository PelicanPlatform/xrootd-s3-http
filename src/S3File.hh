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

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include <fcntl.h>

int parse_path(const S3FileSystem &fs, const char *path,
			   std::string &exposedPath, std::string &object);

class AmazonS3SendMultipartPart;
template <typename T> class AmazonS3NonblockingDownload;
class XrdXrootdGStream;

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

	ssize_t ReadV(XrdOucIOVec *readV, int rdvcnt) override;

	ssize_t Write(const void *buffer, off_t offset, size_t size) override;

	int Write(XrdSfsAio *aiop) override { return -ENOSYS; }

	ssize_t WriteV(XrdOucIOVec *writeV, int wrvcnt) override { return -ENOSYS; }

	int Close(long long *retsz = 0) override;

	size_t getContentLength() { return content_length; }
	time_t getLastModified() { return last_modified; }

	// Launch the global monitor thread associated with S3File objects.
	// Currently, the monitor thread is used to cleanup in-progress file
	// transfers that have been abandoned.
	static void LaunchMonitorThread(XrdSysError &log, XrdOucEnv *envP);

	// Sets the size of the cache entry; defaults to 2MB.
	static void SetCacheEntrySize(size_t size) { m_cache_entry_size = size; }

  private:
	// Periodic cleanup of in-progress transfers.
	//
	// Iterates through the global list of pending multipart uploads
	// that may be paused.  For each, call `Tick` on the upload and
	// see if the transfer has aborted.
	static void Maintenance(XrdSysError &log);

	// Single cleanup run for in-progress transfers.
	static void CleanupTransfersOnce();

	// Send out the statistics to the log or monitoring system.
	static void SendStatistics(XrdSysError &log);

	// Write data while in "streaming mode" where we don't know the
	// ultimate size of the file (and hence can't start streaming
	// partitions immediately).
	ssize_t WriteStreaming(const void *buffer, off_t offset, size_t size);

	// Send a fully-buffered part of the file; only used while in
	// "streaming" mode.
	ssize_t SendPartStreaming();

	ssize_t ContinueSendPart(const void *buffer, size_t size);

	// Download data synchronously, bypassing the cache.
	// The download is only performed if the request size is larger than a cache
	// entry.
	//
	// - `offset`: File offset of the request.
	// - `size`: Size of the request.
	// - `buffer`: Buffer to place resulting data into.
	// - Returns the (offset, size) of any remaining read and `true` if a
	// download occured.
	std::tuple<off_t, size_t, bool> DownloadBypass(off_t offset, size_t size,
												   char *buffer);

	XrdSysError &m_log;
	S3FileSystem *m_oss;

	std::string m_object;
	S3AccessInfo m_ai;

	off_t content_length{-1};
	time_t last_modified{-1};

	static const size_t m_s3_part_size =
		100'000'000; // The size of each S3 chunk.

	// Size of the buffer associated with the cache
	static size_t m_cache_entry_size;

	bool m_is_open{false}; // File open state
	bool m_create{false};
	int partNumber{1};
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

	// The pointer to the "g-stream" monitoring object, if available.
	static XrdXrootdGStream *m_gstream;

	// The double-buffering component for the file handle.  Reads are rounded up
	// to a particular size and kept in the file handle; before requesting new
	// data, the cache is searched to see if the read can be serviced from
	// memory. When possible, a forward prefetch is done
	struct S3Cache {
		struct Entry {
			bool m_failed{false}; // Indication as to whether last download
								  // attempt failed for cache entry.
			bool m_inprogress{
				false}; // Indication as to whether a download is in-progress.
			off_t m_off{-1}; // File offset of the beginning of the cache entry.
							 // -1 signifies unused entry
			size_t m_used{
				0}; // The number of bytes read out of the current cache entry.
			std::vector<char> m_data; // Contents of cache entry
			S3Cache &m_parent;		  // Reference to owning object
			std::unique_ptr<AmazonS3NonblockingDownload<Entry>>
				m_request; // In-progress request to fill entry.

			Entry(S3Cache &cache) : m_parent(cache) {}
			void Download(
				S3File &); // Trigger download request for this cache entry.
			void Notify(); // Notify containing cache that the entry's
						   // in-progress operation has completed.

			// Copy any overlapping data from the cache buffer into the request
			// buffer, returning the remaining data necessary to fill the
			// request.
			//
			// - `req_off`: File offset of the beginning of the request buffer.
			// - `req_size`: Size of the request buffer
			// - `req_buf`: Request buffer to copy data into
			// - `is_hit`: If the request is a cache hit, then OverlapCopy will
			//   increment the hit bytes counter.
			// - Returns the (offset, size) of the remaining reads needed to
			// satisfy the request. If there is only one (or no!) remaining
			// reads, then the corresponding tuple returned is (-1, 0).
			std::tuple<off_t, size_t, off_t, size_t>
			OverlapCopy(off_t req_off, size_t req_size, char *req_buf,
						bool is_hit);
		};
		friend class AmazonS3NonblockingDownload<S3File::S3Cache::Entry>;

		static std::atomic<off_t> m_hit_bytes; // Bytes served from the cache.
		static std::atomic<off_t>
			m_miss_bytes; // Bytes that resulted in a cache miss.
		static std::atomic<off_t>
			m_full_hit_count; // Requests completely served from the cache.
		static std::atomic<off_t>
			m_partial_hit_count; // Requests partially served from the cache.
		static std::atomic<off_t>
			m_miss_count; // Requests that had no data served from the cache.
		static std::atomic<off_t>
			m_bypass_bytes; // Bytes for requests that were large enough they
							// bypassed the cache and fetched directly from S3.
		static std::atomic<off_t>
			m_bypass_count; // Requests that were large enough they (at least
							// partially) bypassed the cache and fetched
							// directly from S3.
		static std::atomic<off_t> m_fetch_bytes; // Bytes that were fetched from
												 // S3 to serve a cache miss.
		static std::atomic<off_t>
			m_fetch_count; // Requests sent to S3 to serve a cache miss.
		static std::atomic<off_t>
			m_unused_bytes; // Bytes that were unused at cache eviction.
		static std::atomic<off_t> m_prefetch_bytes; // Bytes prefetched
		static std::atomic<off_t>
			m_prefetch_count; // Number of prefetch requests
		static std::atomic<off_t>
			m_errors; // Count of errors encountered by cache.
		static std::atomic<std::chrono::steady_clock::duration::rep>
			m_bypass_duration; // Duration of bypass requests.
		static std::atomic<std::chrono::steady_clock::duration::rep>
			m_fetch_duration; // Duration of fetch requests.

		Entry m_a{*this};	// Cache entry A.  Protected by m_mutex.
		Entry m_b{*this};	// Cache entry B.  Protected by m_mutex.
		std::mutex m_mutex; // Mutex protecting the data in the S3Cache object
		std::condition_variable m_cv; // Condition variable for notifying that
									  // new downloaded data is available.

		S3File
			&m_parent; // Reference to the S3File object that owns this cache.

		// Returns `true` if the request offset would be inside the cache entry.
		// The request offset is assumed to be aligned to be inside a single
		// cache entry (that is, smaller than a cache entry and not spanning two
		// entries).
		bool CouldUseAligned(off_t req, off_t cache);

		// Returns true if the specified request, [req_off, req_off + req_size),
		// has any bytes inside the cache entry starting at `cache_off`.
		bool CouldUse(off_t req_off, size_t req_size, off_t cache_off);

		// Trigger the non-blocking download into the cache entries.
		// The condition variable will be notified when one of the caches
		// finishes.
		void DownloadCaches(bool download_a, bool download_b, bool locked);

		// Trigger a blocking read from a given file
		ssize_t Read(char *buffer, off_t offset, size_t size);

		S3Cache(S3File &file) : m_parent(file) {}

		// Shutdown the cache; ensure all reads are completed before
		// deleting the objects.
		~S3Cache();
	};
	S3Cache m_cache{*this};
};
