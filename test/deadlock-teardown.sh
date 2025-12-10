#!/bin/bash
#
# Teardown script for deadlock detection integration tests

set -e

# Get the test name from the command line
TEST_NAME=$1
if [ -z "$TEST_NAME" ]; then
    echo "Usage: $0 <test_name>"
    exit 1
fi

TEST_DIR=${BINARY_DIR}/tests/${TEST_NAME}

# Read the PID file if it exists
if [ -f ${TEST_DIR}/xrootd.pid ]; then
    XROOTD_PID=$(cat ${TEST_DIR}/xrootd.pid)
    
    # Try to kill the process gracefully first
    if kill -0 $XROOTD_PID 2>/dev/null; then
        echo "Stopping XRootD process $XROOTD_PID..."
        kill $XROOTD_PID 2>/dev/null || true
        sleep 1
        
        # Force kill if still running
        if kill -0 $XROOTD_PID 2>/dev/null; then
            echo "Force killing XRootD process $XROOTD_PID..."
            kill -9 $XROOTD_PID 2>/dev/null || true
        fi
    fi
fi

# Clean up the test directory
echo "Cleaning up test directory ${TEST_DIR}..."
rm -rf ${TEST_DIR}

echo "Teardown complete"
