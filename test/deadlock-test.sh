#!/bin/bash
#
# Test script for deadlock detection integration tests

set -e

# Get the test name from the command line
TEST_NAME=$1
if [ -z "$TEST_NAME" ]; then
    echo "Usage: $0 <test_name>"
    exit 1
fi

TEST_DIR=${BINARY_DIR}/tests/${TEST_NAME}

# Read the PID file
XROOTD_PID=$(cat ${TEST_DIR}/xrootd.pid)

echo "Testing deadlock detection with PID $XROOTD_PID..."

# Try to stat the test file, which should trigger a stall
# The dummy OSS will sleep for 10 seconds, but deadlock timeout is 3 seconds
# So the process should be killed within 3-5 seconds

echo "Attempting to access file (this should trigger deadlock detection)..."
timeout 15 curl -f http://localhost:8080/test.txt -o /dev/null 2>&1 || true

# Wait a bit to let the deadlock detection kick in
sleep 2

# Check if the process was killed
if kill -0 $XROOTD_PID 2>/dev/null; then
    echo "ERROR: XRootD process is still running, deadlock detection did not work"
    kill -9 $XROOTD_PID 2>/dev/null || true
    exit 1
fi

echo "SUCCESS: XRootD process was killed by deadlock detection"

# Check if the log file was created
if [ ! -f ${TEST_DIR}/deadlocks.log ]; then
    echo "WARNING: Deadlock log file was not created"
else
    echo "Deadlock log file contents:"
    cat ${TEST_DIR}/deadlocks.log
fi

# Check server log for deadlock message
if grep -q "DEADLOCK DETECTED" ${TEST_DIR}/server.log; then
    echo "SUCCESS: Deadlock message found in server log"
else
    echo "WARNING: Deadlock message not found in server log"
    echo "Server log:"
    cat ${TEST_DIR}/server.log
fi
