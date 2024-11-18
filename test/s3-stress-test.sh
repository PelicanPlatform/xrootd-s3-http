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

IDX=1
while [ $IDX -le 100 ]; do
  IDX=$(($IDX+1))

  curl --cacert $X509_CA_FILE -v --fail "$XROOTD_URL/test-authed/hello_world.txt" 2> "$BINARY_DIR/tests/$TEST_NAME/client-$IDX.log" > "$BINARY_DIR/tests/$TEST_NAME/client-$IDX.out" &
  export CURL_${IDX}_PID=$!

done

IDX=1
while [ $IDX -le 100 ]; do
  IDX=$(($IDX+1))

  CURL_NAME="CURL_${IDX}_PID"
  eval CURL_NAME='\$CURL_${IDX}_PID'
  eval CURL_PID=$CURL_NAME
  wait $CURL_PID
  CURL_EXIT=$?

  if [ $CURL_EXIT -ne 0 ]; then
    echo "Download of hello-world text failed for worker $IDX"
    exit 1
  fi

  CONTENTS=$(cat "$BINARY_DIR/tests/$TEST_NAME/client-$IDX.out")
  if [ "$CONTENTS" != "Hello, World" ]; then
    echo "Downloaded hello-world text for worker $IDX is incorrect: $CONTENTS"
    exit 1
  fi
done
