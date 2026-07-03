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

#include "../src/DeadlockDetector.hh"

#include <XrdSys/XrdSysLogger.hh>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <thread>
#include <unistd.h>
#include <vector>

class DeadlockTest : public testing::Test {
  protected:
	void SetUp() override {
		// Reset the detector state for each test
		DeadlockDetector::SetTimeout(std::chrono::seconds(2));
		DeadlockDetector::SetLogFile("");
	}
};

// Test basic monitor creation and destruction
TEST_F(DeadlockTest, BasicMonitorLifecycle) {
	XrdSysLogger logger;
	XrdSysError log(&logger, "test");

	auto &detector = DeadlockDetector::GetInstance();
	ASSERT_TRUE(detector.Initialize(&log, nullptr));

	{
		DeadlockMonitor monitor("test_operation");
		// Monitor should be added to list
	}
	// Monitor should be removed from list
}

// Test multi-threaded monitor creation/destruction
TEST_F(DeadlockTest, MultiThreadedMonitors) {
	XrdSysLogger logger;
	XrdSysError log(&logger, "test");

	auto &detector = DeadlockDetector::GetInstance();
	ASSERT_TRUE(detector.Initialize(&log, nullptr));

	std::atomic<int> counter{0};
	std::vector<std::thread> threads;

	// Launch multiple threads that rapidly create/destroy monitors
	for (int i = 0; i < 10; ++i) {
		threads.emplace_back([&counter]() {
			for (int j = 0; j < 1000; ++j) {
				DeadlockMonitor monitor("rapid_test");
				counter.fetch_add(1, std::memory_order_relaxed);
			}
		});
	}

	// Wait for all threads to complete
	for (auto &t : threads) {
		t.join();
	}

	EXPECT_EQ(counter.load(), 10000);
}

// Test that monitors don't trigger when below timeout
TEST_F(DeadlockTest, NoTriggerBelowTimeout) {
	XrdSysLogger logger;
	XrdSysError log(&logger, "test");

	DeadlockDetector::SetTimeout(std::chrono::seconds(5));
	auto &detector = DeadlockDetector::GetInstance();
	ASSERT_TRUE(detector.Initialize(&log, nullptr));

	{
		DeadlockMonitor monitor("short_operation");
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	// Wait a bit to let the monitor thread run
	std::this_thread::sleep_for(std::chrono::milliseconds(500));

	// Process should still be alive (no SIGKILL)
	SUCCEED();
}

// Death test: Monitor that exceeds timeout should trigger SIGKILL
TEST_F(DeadlockTest, TriggerDeadlockDetection) {
	GTEST_FLAG_SET(death_test_style, "threadsafe");

	EXPECT_EXIT(
		{
			XrdSysLogger logger;
			XrdSysError log(&logger, "test");

			DeadlockDetector::SetTimeout(std::chrono::milliseconds(500));
			auto &detector = DeadlockDetector::GetInstance();
			detector.Initialize(&log, nullptr);

			DeadlockMonitor monitor("long_blocking_operation");
			// Sleep longer than the timeout
			std::this_thread::sleep_for(std::chrono::seconds(3));
		},
		::testing::KilledBySignal(SIGKILL), ".*");
}

// When a deadlock triggers and a log file is configured, the event must be
// recorded in that file before the process is killed.  The death-test child
// writes the file; the parent inspects it after the child dies.  A fixed path
// is used so both processes agree on it across the threadsafe re-exec.
TEST_F(DeadlockTest, WritesEventToLogFile) {
	const char *logpath = "/tmp/deadlock_unit_event.log";
	unlink(logpath);

	GTEST_FLAG_SET(death_test_style, "threadsafe");
	EXPECT_EXIT(
		{
			XrdSysLogger logger;
			XrdSysError log(&logger, "test");

			DeadlockDetector::SetTimeout(std::chrono::milliseconds(500));
			DeadlockDetector::SetLogFile(logpath);
			auto &detector = DeadlockDetector::GetInstance();
			detector.Initialize(&log, nullptr);

			DeadlockMonitor monitor("logged_operation");
			std::this_thread::sleep_for(std::chrono::seconds(3));
		},
		::testing::KilledBySignal(SIGKILL), ".*");

	std::ifstream f(logpath);
	ASSERT_TRUE(f.is_open()) << "deadlock event log was not created";
	std::stringstream buf;
	buf << f.rdbuf();
	const std::string contents = buf.str();
	EXPECT_NE(contents.find("DEADLOCK DETECTED"), std::string::npos);
	EXPECT_NE(contents.find("logged_operation"), std::string::npos);
	unlink(logpath);
}

// Test configuration parsing
TEST_F(DeadlockTest, ConfigurationParsing) {
	XrdSysLogger logger;
	XrdSysError log(&logger, "test");

	// Create a temporary config file
	const char *config_content = "deadlock.timeout 10\n"
								 "deadlock.logfile /tmp/deadlock.log\n";

	char tmp_config[] = "/tmp/deadlock_test_XXXXXX";
	int fd = mkstemp(tmp_config);
	ASSERT_NE(fd, -1);
	write(fd, config_content, strlen(config_content));
	close(fd);

	auto &detector = DeadlockDetector::GetInstance();
	ASSERT_TRUE(detector.Initialize(&log, tmp_config));

	EXPECT_EQ(DeadlockDetector::GetTimeout(), std::chrono::seconds(10));

	unlink(tmp_config);
}

// Test monitor operations with different names
TEST_F(DeadlockTest, MonitorWithDifferentOperations) {
	XrdSysLogger logger;
	XrdSysError log(&logger, "test");

	auto &detector = DeadlockDetector::GetInstance();
	ASSERT_TRUE(detector.Initialize(&log, nullptr));

	{
		DeadlockMonitor monitor1("read");
		DeadlockMonitor monitor2("write");
		DeadlockMonitor monitor3("stat");
	}

	SUCCEED();
}

// Test creating monitor with initialization
TEST_F(DeadlockTest, MonitorAfterInitialization) {
	XrdSysLogger logger;
	XrdSysError log(&logger, "test");

	auto &detector = DeadlockDetector::GetInstance();
	ASSERT_TRUE(detector.Initialize(&log, nullptr));

	DeadlockMonitor monitor("test");
	std::this_thread::sleep_for(std::chrono::milliseconds(100));
	// Should not crash
	SUCCEED();
}

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);

	// XrdOucGatherConf (used to parse the deadlock.* directives) requires the
	// XRDINSTANCE environment variable to be set, as it would be inside a
	// running xrootd.  Set it here so configuration-parsing tests work.
	setenv("XRDINSTANCE", "xrootd", 1);

	return RUN_ALL_TESTS();
}
