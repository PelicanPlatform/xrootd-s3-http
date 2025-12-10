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

#include "DeadlockDetector.hh"

#include <XrdOuc/XrdOucGatherConf.hh>
#include <XrdSys/XrdSysError.hh>

#include <csignal>
#include <ctime>
#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <thread>
#include <unistd.h>

#ifdef __linux__
#include <sched.h>
#endif

// Static member initialization
std::chrono::steady_clock::duration DeadlockDetector::m_timeout =
	std::chrono::seconds(300); // Default 5 minutes
std::string DeadlockDetector::m_log_file;
std::once_flag DeadlockDetector::m_init_flag;
std::mutex DeadlockDetector::m_shutdown_mutex;
std::condition_variable DeadlockDetector::m_shutdown_cv;
std::atomic<bool> DeadlockDetector::m_shutdown_requested{false};
std::atomic<bool> DeadlockDetector::m_initialized{false};
std::thread DeadlockDetector::m_monitor_thread;

DeadlockDetector::DeadlockDetector() {}

DeadlockDetector::~DeadlockDetector() {}

DeadlockDetector &DeadlockDetector::GetInstance() {
	static DeadlockDetector instance;
	return instance;
}

bool DeadlockDetector::Initialize(XrdSysError *log, const char *configfn) {
	m_log = log;

	if (!configfn) {
		if (m_log) {
			m_log->Emsg("DeadlockDetector",
						"No configuration file provided, using defaults");
		}
		return true;
	}

	// Parse configuration
	XrdOucGatherConf conf("deadlock.timeout deadlock.logfile", m_log);
	int result = conf.Gather(configfn, XrdOucGatherConf::trim_lines);
	if (result < 0) {
		if (m_log) {
			m_log->Emsg("DeadlockDetector", -result, "parsing config file",
						configfn);
		}
		return false;
	}

	char *val;
	while (conf.GetLine()) {
		val = conf.GetToken();
		if (!strcmp(val, "timeout")) {
			if (!(val = conf.GetToken())) {
				if (m_log) {
					m_log->Emsg("DeadlockDetector",
								"deadlock.timeout requires an argument "
								"(timeout in seconds)");
				}
				return false;
			}
			try {
				int timeout_secs = std::stoi(val);
				if (timeout_secs <= 0) {
					if (m_log) {
						m_log->Emsg("DeadlockDetector",
									"deadlock.timeout must be positive");
					}
					return false;
				}
				SetTimeout(std::chrono::seconds(timeout_secs));
			} catch (const std::exception &e) {
				if (m_log) {
					m_log->Emsg("DeadlockDetector",
								"Invalid timeout value:", val);
				}
				return false;
			}
		} else if (!strcmp(val, "logfile")) {
			if (!(val = conf.GetToken())) {
				if (m_log) {
					m_log->Emsg("DeadlockDetector",
								"deadlock.logfile requires an argument (file "
								"path)");
				}
				return false;
			}
			SetLogFile(val);
		}
	}

	// Start the monitor thread
	std::call_once(m_init_flag, [this] {
		m_initialized.store(true, std::memory_order_release);
		m_monitor_thread = std::thread(MonitorThread, this);
	});

	return true;
}

void DeadlockDetector::MonitorThread(DeadlockDetector *detector) {
	while (true) {
		{
			std::unique_lock<std::mutex> lock(m_shutdown_mutex);
			if (m_shutdown_cv.wait_for(lock, std::chrono::seconds(1), [] {
					return m_shutdown_requested.load(std::memory_order_acquire);
				})) {
				break;
			}
		}

		if (detector->m_log) {
			detector->CheckDeadlocks();
		}
	}
}

void DeadlockDetector::CheckDeadlocks() {
	auto now = std::chrono::steady_clock::now();

	for (int i = 0; i < NUM_LISTS; ++i) {
		std::unique_lock<std::mutex> lock(m_lists[i].m_mutex);
		MonitorNode *node = m_lists[i].m_first;

		while (node) {
			auto elapsed = now - node->m_start_time;
			if (elapsed > m_timeout) {
				// Deadlock detected
				std::stringstream ss;
				ss << "DEADLOCK DETECTED: Operation '";
				if (node->m_operation) {
					ss << node->m_operation;
				} else {
					ss << "<unknown>";
				}
				ss << "' has been blocked for "
				   << std::chrono::duration_cast<std::chrono::seconds>(elapsed)
						  .count()
				   << " seconds (timeout: "
				   << std::chrono::duration_cast<std::chrono::seconds>(
						  m_timeout)
						  .count()
				   << " seconds)";

				if (m_log) {
					m_log->Emsg("DeadlockDetector", ss.str().c_str());
				}

				// Write to log file if configured
				if (!m_log_file.empty()) {
					try {
						int fd = open(m_log_file.c_str(),
									  O_WRONLY | O_CREAT | O_APPEND, 0644);
						if (fd >= 0) {
							auto t = std::time(nullptr);
							std::string timestamp = std::ctime(&t);
							// Remove trailing newline from ctime
							if (!timestamp.empty() &&
								timestamp.back() == '\n') {
								timestamp.pop_back();
							}
							std::string log_entry = timestamp + ": " +
													ss.str() + "\n";
							write(fd, log_entry.c_str(), log_entry.size());
							close(fd);
						}
					} catch (...) {
						// Ignore errors writing to log file
					}
				}

				// Kill the process
				kill(getpid(), SIGKILL);
				return; // Won't reach here, but for clarity
			}
			node = node->m_next;
		}
	}
}

void DeadlockDetector::Shutdown() {
	m_shutdown_requested.store(true, std::memory_order_release);
	m_shutdown_cv.notify_one();

	// Join the monitor thread
	if (m_monitor_thread.joinable()) {
		m_monitor_thread.join();
	}
}

// DeadlockMonitor implementation

DeadlockMonitor::DeadlockMonitor(const char *operation) {
	m_node.m_start_time = std::chrono::steady_clock::now();
	m_node.m_operation = operation;

	// Select list based on CPU ID
	m_list_id = GetCpuId() % DeadlockDetector::NUM_LISTS;

	// Add to the list
	auto &detector = DeadlockDetector::GetInstance();
	auto &list_head = detector.m_lists[m_list_id];
	std::unique_lock<std::mutex> lock(list_head.m_mutex);

	m_node.m_next = list_head.m_first;
	if (m_node.m_next) {
		m_node.m_next->m_prev = &m_node;
	}
	list_head.m_first = &m_node;
}

DeadlockMonitor::~DeadlockMonitor() {
	// Remove from the list
	auto &detector = DeadlockDetector::GetInstance();
	auto &list_head = detector.m_lists[m_list_id];
	std::unique_lock<std::mutex> lock(list_head.m_mutex);

	if (m_node.m_prev) {
		m_node.m_prev->m_next = m_node.m_next;
	} else {
		list_head.m_first = m_node.m_next;
	}

	if (m_node.m_next) {
		m_node.m_next->m_prev = m_node.m_prev;
	}
}

int DeadlockMonitor::GetCpuId() {
#ifdef __linux__
	int cpu = sched_getcpu();
	return (cpu >= 0) ? cpu : 0;
#else
	return 0;
#endif
}
