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

echo "Running $TEST_NAME - N2N prefix mapping tests"

if [ ! -f "$BINARY_DIR/tests/$TEST_NAME/setup.sh" ]; then
  echo "Test environment file $BINARY_DIR/tests/$TEST_NAME/setup.sh does not exist - cannot run test"
  exit 1
fi
. "$BINARY_DIR/tests/$TEST_NAME/setup.sh"

FAILED=0

######################################
# Test 1: Access file via logical path
######################################
echo "Test 1: Access file via logical path (/logical/hello_world.txt -> /physical/hello_world.txt)"

CONTENTS=$(curl --cacert $X509_CA_FILE -v --fail "$XROOTD_URL/logical/hello_world.txt" 2> "$BINARY_DIR/tests/$TEST_NAME/client.log")
CURL_EXIT=$?

if [ $CURL_EXIT -ne 0 ]; then
  echo "FAIL: Download via logical path failed"
  FAILED=1
else
  if [ "$CONTENTS" != "Hello, World" ]; then
    echo "FAIL: Downloaded content is incorrect: $CONTENTS"
    FAILED=1
  else
    echo "PASS: File accessed via logical path"
  fi
fi

######################################
# Test 2: Access file in subdirectory via logical path
######################################
echo "Test 2: Access file in subdir via logical path (/logical/subdir/test.txt)"

CONTENTS=$(curl --cacert $X509_CA_FILE -v --fail "$XROOTD_URL/logical/subdir/test.txt" 2>> "$BINARY_DIR/tests/$TEST_NAME/client.log")
CURL_EXIT=$?

if [ $CURL_EXIT -ne 0 ]; then
  echo "FAIL: Download via logical path (subdir) failed"
  FAILED=1
else
  if [ "$CONTENTS" != "Test file in subdir" ]; then
    echo "FAIL: Downloaded content is incorrect: $CONTENTS"
    FAILED=1
  else
    echo "PASS: File in subdir accessed via logical path"
  fi
fi

######################################
# Test 3: Access file directly via physical path (should also work)
######################################
echo "Test 3: Access file directly via physical path (/physical/hello_world.txt)"

CONTENTS=$(curl --cacert $X509_CA_FILE -v --fail "$XROOTD_URL/physical/hello_world.txt" 2>> "$BINARY_DIR/tests/$TEST_NAME/client.log")
CURL_EXIT=$?

if [ $CURL_EXIT -ne 0 ]; then
  echo "FAIL: Download via physical path failed"
  FAILED=1
else
  if [ "$CONTENTS" != "Hello, World" ]; then
    echo "FAIL: Downloaded content is incorrect: $CONTENTS"
    FAILED=1
  else
    echo "PASS: File accessed via physical path"
  fi
fi

######################################
# Test 4: Missing file via logical path should return 404
######################################
echo "Test 4: Missing file via logical path returns 404"

HTTP_CODE=$(curl --cacert $X509_CA_FILE --output /dev/null -v --write-out '%{http_code}' "$XROOTD_URL/logical/missing.txt" 2>> "$BINARY_DIR/tests/$TEST_NAME/client.log")
if [ "$HTTP_CODE" -ne 404 ]; then
  echo "FAIL: Expected HTTP code 404; actual was $HTTP_CODE"
  FAILED=1
else
  echo "PASS: Missing file returns 404"
fi

######################################
# Test 5: Access via strict path
######################################
echo "Test 5: Access file via strict path (/strict/hello_world.txt)"

CONTENTS=$(curl --cacert $X509_CA_FILE -v --fail "$XROOTD_URL/strict/hello_world.txt" 2>> "$BINARY_DIR/tests/$TEST_NAME/client.log")
CURL_EXIT=$?

if [ $CURL_EXIT -ne 0 ]; then
  echo "FAIL: Download via strict path failed"
  FAILED=1
else
  if [ "$CONTENTS" != "Hello, World" ]; then
    echo "FAIL: Downloaded content is incorrect: $CONTENTS"
    FAILED=1
  else
    echo "PASS: File accessed via strict path"
  fi
fi

######################################
# Test 6: URL with trailing slash (directory listing or redirect)
######################################
echo "Test 6: URL with trailing slash (/logical/subdir/)"

# Just check that it doesn't error catastrophically (might return 403 if no dir listing, which is OK)
HTTP_CODE=$(curl --cacert $X509_CA_FILE --output /dev/null -v --write-out '%{http_code}' "$XROOTD_URL/logical/subdir/" 2>> "$BINARY_DIR/tests/$TEST_NAME/client.log")
# Accept 200, 403, or 404 as valid responses (server config dependent)
if [ "$HTTP_CODE" -ne 200 ] && [ "$HTTP_CODE" -ne 403 ] && [ "$HTTP_CODE" -ne 404 ]; then
  echo "FAIL: Unexpected HTTP code for directory URL: $HTTP_CODE"
  FAILED=1
else
  echo "PASS: Directory URL handled (HTTP $HTTP_CODE)"
fi

######################################
# Test 7: Unmapped path passes through unchanged
######################################
echo "Test 7: Unmapped path passes through unchanged (/unmapped/test.txt returns 404)"

HTTP_CODE=$(curl --cacert $X509_CA_FILE --output /dev/null -v --write-out '%{http_code}' "$XROOTD_URL/unmapped/test.txt" 2>> "$BINARY_DIR/tests/$TEST_NAME/client.log")
if [ "$HTTP_CODE" -ne 404 ]; then
  echo "FAIL: Expected HTTP code 404 for unmapped path; actual was $HTTP_CODE"
  FAILED=1
else
  echo "PASS: Unmapped path returns 404"
fi

######################################
# Summary
######################################
if [ $FAILED -ne 0 ]; then
  echo ""
  echo "========================================="
  echo "SOME TESTS FAILED - see above for details"
  echo "========================================="
  echo ""
  echo "========================================="
  echo "SERVER LOG:"
  echo "========================================="
  cat "$BINARY_DIR/tests/$TEST_NAME/server.log"
  echo ""
  echo "========================================="
  echo "CLIENT LOG:"
  echo "========================================="
  cat "$BINARY_DIR/tests/$TEST_NAME/client.log"
  exit 1
else
  echo ""
  echo "========================================="
  echo "ALL N2N INTEGRATION TESTS PASSED"
  echo "========================================="
  exit 0
fi
