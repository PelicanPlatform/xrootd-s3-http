#!/bin/sh
#
# Deadlock-detection integration test.
#
#  1. Positive: a normal request completes well under the timeout and the
#     server keeps running (deadlock detection does not fire spuriously).
#  2. Negative: a request that stalls past the timeout causes the deadlock
#     monitor to SIGKILL the xrootd process; we confirm the process died and
#     that the event was logged.

TEST_NAME=$1

if [ -z "$BINARY_DIR" ]; then
  echo "\$BINARY_DIR environment variable is not set; cannot run test"
  exit 1
fi

TEST_DIR="$BINARY_DIR/tests/$TEST_NAME"
if [ ! -f "$TEST_DIR/setup.sh" ]; then
  echo "Setup file $TEST_DIR/setup.sh not found; setup must run first"
  exit 1
fi
. "$TEST_DIR/setup.sh"

CURL="curl -s --cacert $X509_CA_FILE"

########################################
# Positive test: normal access succeeds.
########################################
echo "Positive test: fetching hello_world.txt (should succeed, no kill)..."
CONTENTS=$($CURL --fail "${XROOTD_URL}hello_world.txt" 2>>"$TEST_DIR/client.log")
if [ "$CONTENTS" != "Hello, World" ]; then
  echo "ERROR: normal download failed or returned unexpected content: '$CONTENTS'"
  exit 1
fi
if ! kill -0 "$XROOTD_PID" 2>/dev/null; then
  echo "ERROR: server was killed by a normal (below-threshold) request"
  exit 1
fi
echo "Positive test passed: normal request served and server still alive."

########################################
# Negative test: a stalling access trips the detector.
########################################
echo "Negative test: fetching stall_me.txt (should trigger SIGKILL)..."
# The OSS stalls this Stat for 30s; the deadlock timeout is 3s.  The request
# will not complete -- cap the client so the test cannot hang.
timeout 20 $CURL --fail "${XROOTD_URL}stall_me.txt" -o /dev/null 2>>"$TEST_DIR/client.log"

# Give the monitor a moment past the timeout to act.
IDX=0
while kill -0 "$XROOTD_PID" 2>/dev/null; do
  sleep 1
  IDX=$((IDX+1))
  if [ $IDX -ge 15 ]; then
    echo "ERROR: xrootd (PID $XROOTD_PID) still running; deadlock detection did not fire"
    exit 1
  fi
done
echo "Server was killed as expected after ~${IDX}s."

# Confirm it was actually SIGKILL from the detector, not something else.
if grep -q "DEADLOCK DETECTED" "$SERVER_LOG"; then
  echo "Found 'DEADLOCK DETECTED' in the server log."
else
  echo "ERROR: 'DEADLOCK DETECTED' not found in server log"
  echo "----- server log tail -----"
  tail -n 40 "$SERVER_LOG"
  exit 1
fi

if [ -s "$DEADLOCK_LOG" ]; then
  echo "Deadlock event log written:"
  cat "$DEADLOCK_LOG"
else
  echo "ERROR: deadlock event log $DEADLOCK_LOG was not written"
  exit 1
fi

echo "SUCCESS: deadlock detection integration test passed."
