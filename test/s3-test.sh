#!/bin/sh

TEST_NAME=$1

if [ -z "$BINARY_DIR" ]; then
  echo "\$BINARY_DIR environment variable is not set; cannot run test"
  exit 1
fi
if [ ! -d "$BINARY_DIR" ]; then
  echo "$BINARY_DIR is not a directory; cannot run test"
  exit 1
fi

echo "Running $TEST_NAME - simple download"

if [ ! -f "$BINARY_DIR/tests/$TEST_NAME/setup.sh" ]; then
  echo "Test environment file $BINARY_DIR/tests/$TEST_NAME/setup.sh does not exist - cannot run test"
  exit 1
fi
. "$BINARY_DIR/tests/$TEST_NAME/setup.sh"

if [ -z "$XROOTD_URL" ]; then
  echo "XRootD URL is not set; cannot test"
  exit 1
fi

CONTENTS=$(curl --cacert $X509_CA_FILE -v --fail "$XROOTD_URL/test/hello_world.txt" 2> "$BINARY_DIR/tests/$TEST_NAME/client.log")
CURL_EXIT=$?

if [ $CURL_EXIT -ne 0 ]; then
  echo "Download of hello-world text failed"
  exit 1
fi

if [ "$CONTENTS" != "Hello, World" ]; then
  echo "Downloaded hello-world text is incorrect: $CONTENTS"
  exit 1
fi

echo "Running $TEST_NAME - missing object"

HTTP_CODE=$(curl --cacert $X509_CA_FILE --output /dev/null -v --write-out '%{http_code}' "$XROOTD_URL/test/missing.txt" 2>> "$BINARY_DIR/tests/$TEST_NAME/client.log")
if [ "$HTTP_CODE" -ne 404 ]; then
  echo "Expected HTTP code is 404; actual was $HTTP_CODE"
  exit 1
fi

echo "Running $TEST_NAME - filtered prefix"

HTTP_CODE=$(curl --cacert $X509_CA_FILE --output /dev/null -v --write-out '%{http_code}' "$XROOTD_URL/test2/hello_world.txt" 2>> "$BINARY_DIR/tests/$TEST_NAME/filter.log")

if [ "$HTTP_CODE" -ne 200 ]; then
  echo "Expected HTTP code is 200; actual was $HTTP_CODE"
  exit 1
fi

HTTP_CODE=$(curl --cacert $X509_CA_FILE --output /dev/null -v --write-out '%{http_code}' "$XROOTD_URL/test2/hello_world2.txt" 2>> "$BINARY_DIR/tests/$TEST_NAME/filter.log")

if [ "$HTTP_CODE" -ne 404 ]; then
  echo "Expected HTTP code is 404; actual was $HTTP_CODE"
  exit 1
fi

echo "Running $TEST_NAME - PROPFIND href paths"

PROPFIND_XML="$BINARY_DIR/tests/$TEST_NAME/propfind_testfolder.xml"
HTTP_CODE=$(curl --cacert $X509_CA_FILE --silent --show-error --request PROPFIND --header 'Depth: 1' --output "$PROPFIND_XML" --write-out '%{http_code}' "$XROOTD_URL/test/testfolder" 2>> "$BINARY_DIR/tests/$TEST_NAME/client.log")
if [ "$HTTP_CODE" -ne 207 ]; then
  echo "Expected PROPFIND HTTP code is 207; actual was $HTTP_CODE"
  exit 1
fi

if ! grep -q '<D:href>/test/testfolder</D:href>' "$PROPFIND_XML"; then
  echo "Missing expected directory href in PROPFIND response"
  cat "$PROPFIND_XML"
  exit 1
fi
if ! grep -q '<D:href>/test/testfolder/file1.txt</D:href>' "$PROPFIND_XML"; then
  echo "Missing expected file1 href in PROPFIND response"
  cat "$PROPFIND_XML"
  exit 1
fi
if ! grep -q '<D:href>/test/testfolder/file2.txt</D:href>' "$PROPFIND_XML"; then
  echo "Missing expected file2 href in PROPFIND response"
  cat "$PROPFIND_XML"
  exit 1
fi
if grep -q 'testfolder%2F' "$PROPFIND_XML"; then
  echo "Found encoded duplicate directory prefix in PROPFIND response"
  cat "$PROPFIND_XML"
  exit 1
fi

