#!/bin/sh
#
# Teardown for the deadlock-detection integration test.

TEST_NAME=$1

if [ -z "$BINARY_DIR" ]; then
  echo "\$BINARY_DIR environment variable is not set; cannot run teardown"
  exit 1
fi

TEST_DIR="$BINARY_DIR/tests/$TEST_NAME"

if [ -f "$TEST_DIR/setup.sh" ]; then
  . "$TEST_DIR/setup.sh"
  if [ -n "$XROOTD_PID" ] && kill -0 "$XROOTD_PID" 2>/dev/null; then
    echo "Stopping xrootd process $XROOTD_PID..."
    kill "$XROOTD_PID" 2>/dev/null
    sleep 1
    kill -9 "$XROOTD_PID" 2>/dev/null || true
  fi
fi

echo "Teardown complete"
