#!/bin/bash
#
# Setup script for deadlock detection integration tests

set -e

# Get the test name from the command line
TEST_NAME=$1
if [ -z "$TEST_NAME" ]; then
    echo "Usage: $0 <test_name>"
    exit 1
fi

# Setup environment
TEST_DIR=${BINARY_DIR}/tests/${TEST_NAME}
mkdir -p ${TEST_DIR}

# Create a temporary directory for XRootD
XROOTD_DIR=${TEST_DIR}/xrootd
mkdir -p ${XROOTD_DIR}/data

# Create the configuration file
cat > ${TEST_DIR}/xrootd.cfg <<EOF
all.export /
xrd.protocol http:8080 libXrdHttp.so
oss.localroot ${XROOTD_DIR}/data

# Stack deadlock detection on top of dummy stall OSS
ofs.osslib ++ ${BINARY_DIR}/libXrdOssDeadlock-5.so
ofs.osslib ${BINARY_DIR}/libXrdOssDummyStall-5.so

# Configure deadlock detection with short timeout for testing
deadlock.timeout 3
deadlock.logfile ${TEST_DIR}/deadlocks.log

# Disable TLS certificate verification for testing
xrd.tls off capable
EOF

# Create a test file
echo "test content" > ${XROOTD_DIR}/data/test.txt

# Start xrootd
echo "Starting XRootD on port 8080..."
cd ${TEST_DIR}
${XROOTD_BINDIR}/xrootd -c ${TEST_DIR}/xrootd.cfg -k fifo -l ${TEST_DIR}/server.log -n ${TEST_NAME} &
XROOTD_PID=$!
echo $XROOTD_PID > ${TEST_DIR}/xrootd.pid

# Wait for server to start
sleep 2

# Check if server is running
if ! kill -0 $XROOTD_PID 2>/dev/null; then
    echo "XRootD failed to start"
    cat ${TEST_DIR}/server.log
    exit 1
fi

echo "XRootD started with PID $XROOTD_PID"
