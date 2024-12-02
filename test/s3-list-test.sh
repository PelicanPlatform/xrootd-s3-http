#!/bin/sh

############################
#        Test Setup        #
############################
TEST_NAME=$1

if [ -z "$BINARY_DIR" ]; then
  echo "\$BINARY_DIR environment variable is not set; cannot run test"
  exit 1
fi
if [ ! -d "$BINARY_DIR" ]; then
  echo "$BINARY_DIR is not a directory; cannot run test"
  exit 1
fi

echo "Running $TEST_NAME - PROPFIND test"

if [ ! -f "$BINARY_DIR/tests/$TEST_NAME/setup.sh" ]; then
  echo "Test environment file $BINARY_DIR/tests/$TEST_NAME/setup.sh does not exist - cannot run test"
  exit 1
fi
. "$BINARY_DIR/tests/$TEST_NAME/setup.sh"

if [ -z "$XROOTD_URL" ]; then
  echo "XRootD URL is not set; cannot test"
  exit 1
fi

############################
#     Start the tests      #
############################

# PROPFIND against public bucket with `hello` prefix
RESPONSE_PUBLIC=$(curl --cacert "$X509_CA_FILE" -v --fail -X PROPFIND "$XROOTD_URL/test-public/hello" \
  2>"$BINARY_DIR/tests/$TEST_NAME/propfind-client-public.log")
CURL_EXIT_PUBLIC=$?

if [ $CURL_EXIT_PUBLIC -ne 0 ]; then
  echo "PROPFIND request against public bucket failed: CURL exit code $CURL_EXIT_PUBLIC"
  exit 1
fi

# TODO: Adjust this XML when when lists are fixed -- this will fail until then
# Validate the public bucket response contains the expected XML
EXPECTED_XML_PUBLIC='<?xml version="1.0" encoding="UTF-8"?><D:multistatus xmlns:D="DAV:"><D:response><D:href>/test/hello_world.txt</D:href></D:response></D:multistatus>'
if [ "$RESPONSE_PUBLIC" != "$EXPECTED_XML_PUBLIC" ]; then
  echo "PROPFIND response for public bucket does not match expected output"
  echo "Actual response: $RESPONSE_PUBLIC"
  exit 1
fi

echo "Public PROPFIND test passed"

# PROPFIND against authed bucket with `hello` prefix
RESPONSE_AUTHED=$(curl --cacert "$X509_CA_FILE" -v --fail -X PROPFIND "$XROOTD_URL/test-authed/hello" \
  2>"$BINARY_DIR/tests/$TEST_NAME/propfind-client-authed.log")
CURL_EXIT_AUTHED=$?

if [ $CURL_EXIT_AUTHED -ne 0 ]; then
  echo "PROPFIND request against authed bucket failed"
  exit 1
fi

# TODO: Adjust this XML when when lists are fixed -- this will fail until then
# Validate the authed bucket response contains the expected XML
EXPECTED_XML_AUTHED='<?xml version="1.0" encoding="UTF-8"?><D:multistatus xmlns:D="DAV:"><D:response><D:href>/test/hello_world.txt</D:href></D:response></D:multistatus>'
if [ "$RESPONSE_AUTHED" != "$EXPECTED_XML_AUTHED" ]; then
  echo "PROPFIND response for authed bucket does not match expected output"
  echo "Actual response: $RESPONSE_AUTHED"
  exit 1
fi

echo "Authed PROPFIND test passed"
