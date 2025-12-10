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

#ifndef __DEADLOCK_DETECTOR_HH_
#define __DEADLOCK_DETECTOR_HH_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>

// Forward declarations
class XrdSysError;

/**
 * Core deadlock detection system that monitors blocking operations.
 *
 * On module load, creates a background thread that periodically checks
 * for operations that exceed a configured timeout threshold. If a deadlock
 * is detected, logs an error and kills the process with SIGKILL.
 *
 * Uses per-CPU lists (15 on Linux, 1 on other platforms) to reduce lock
 * contention when creating/destroying monitors.
 */
class DeadlockDetector {
  public:
	// Get the singleton instance
	static DeadlockDetector &GetInstance();

	// Initialize the detector with configuration
	// Must be called before any monitors are created
	bool Initialize(XrdSysError *log, const char *configfn);

	// Set the timeout threshold for deadlock detection
	static void SetTimeout(std::chrono::steady_clock::duration timeout) {
		m_timeout = timeout;
	}

	// Set the optional log file for deadlock events
	static void SetLogFile(const std::string &path) { m_log_file = path; }

	// Get the current timeout
	static std::chrono::steady_clock::duration GetTimeout() {
		return m_timeout;
	}

  private:
	DeadlockDetector();
	~DeadlockDetector();

	// Prevent copying
	DeadlockDetector(const DeadlockDetector &) = delete;
	DeadlockDetector &operator=(const DeadlockDetector &) = delete;

	// Background thread that monitors for deadlocks
	static void MonitorThread(DeadlockDetector *detector);

	// Check all lists for deadlocks
	void CheckDeadlocks();

	// Invoked on library shutdown to cleanly exit the monitor thread
	static void Shutdown() __attribute__((destructor));

	// Node in the doubly-linked list
	struct MonitorNode {
		MonitorNode *m_prev{nullptr};
		MonitorNode *m_next{nullptr};
		std::chrono::steady_clock::time_point m_start_time;
		const char *m_operation{nullptr};
	};

	// Cache-line aligned list head to avoid false sharing
	struct alignas(64) ListHead {
		std::mutex m_mutex;
		MonitorNode *m_first{nullptr};
	};

#ifdef __linux__
	static constexpr int NUM_LISTS = 15;
#else
	static constexpr int NUM_LISTS = 1;
#endif

	// Array of list heads
	ListHead m_lists[NUM_LISTS];

	// Logger instance
	XrdSysError *m_log{nullptr};

	// Timeout threshold for deadlock detection
	static std::chrono::steady_clock::duration m_timeout;

	// Optional log file for deadlock events
	static std::string m_log_file;

	// Thread management
	static std::once_flag m_init_flag;
	static std::mutex m_shutdown_mutex;
	static std::condition_variable m_shutdown_cv;
	static std::atomic<bool> m_shutdown_requested;
	static std::atomic<bool> m_initialized;

	friend class DeadlockMonitor;
};

/**
 * RAII monitor object that tracks a single blocking operation.
 *
 * Creates a node in the appropriate list on construction and removes
 * it on destruction. The background thread checks these nodes to detect
 * operations that exceed the timeout threshold.
 */
class DeadlockMonitor {
  public:
	explicit DeadlockMonitor(const char *operation = nullptr);
	~DeadlockMonitor();

	// Prevent copying and moving
	DeadlockMonitor(const DeadlockMonitor &) = delete;
	DeadlockMonitor &operator=(const DeadlockMonitor &) = delete;
	DeadlockMonitor(DeadlockMonitor &&) = delete;
	DeadlockMonitor &operator=(DeadlockMonitor &&) = delete;

  private:
	// Get the CPU ID for list selection
	static int GetCpuId();

	// Node in the linked list
	DeadlockDetector::MonitorNode m_node;

	// Which list this monitor belongs to
	int m_list_id;
};

#endif // __DEADLOCK_DETECTOR_HH_
