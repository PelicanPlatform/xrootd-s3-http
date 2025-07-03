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
if [ -z "$SOURCE_DIR" ]; then
  echo "\$SOURCE_DIR environment variable is not set; cannot run test"
  exit 1
fi
if [ ! -d "$SOURCE_DIR" ]; then
  echo "\$SOURCE_DIR environment variable is not set; cannot run test"
  exit 1
fi

echo "Setting up HTTP server for $TEST_NAME test"

XROOTD_BIN="$XROOTD_BINDIR/xrootd"
if [ -z "XROOTD_BIN" ]; then
  echo "xrootd binary not found; cannot run unit test"
  exit 1
fi

mkdir -p "$BINARY_DIR/tests/$TEST_NAME"
RUNDIR=$(mktemp -d -p "$BINARY_DIR/tests/$TEST_NAME" test_run.XXXXXXXX)

if [ ! -d "$RUNDIR" ]; then
  echo "Failed to create test run directory; cannot run xrootd"
  exit 1
fi

echo "Using $RUNDIR as the test run's home directory."
cd "$RUNDIR"

export XROOTD_CONFIGDIR="$RUNDIR/xrootd-config"
mkdir -p "$XROOTD_CONFIGDIR/ca"

echo > "$BINARY_DIR/tests/$TEST_NAME/server.log"

# Create the TLS credentials for the test
openssl genrsa -out "$XROOTD_CONFIGDIR/tlscakey.pem" 4096 >> "$BINARY_DIR/tests/$TEST_NAME/server.log"
touch "$XROOTD_CONFIGDIR/ca/index.txt"
echo '01' > "$XROOTD_CONFIGDIR/ca/serial.txt"

cat > "$XROOTD_CONFIGDIR/tlsca.ini" <<EOF

[ ca ]
default_ca = CA_test

[ CA_test ]

default_days = 365
default_md = sha256
private_key = $XROOTD_CONFIGDIR/tlscakey.pem
certificate = $XROOTD_CONFIGDIR/tlsca.pem
new_certs_dir = $XROOTD_CONFIGDIR/ca
database = $XROOTD_CONFIGDIR/ca/index.txt
serial = $XROOTD_CONFIGDIR/ca/serial.txt

[ req ]
default_bits = 4096
distinguished_name = ca_test_dn
x509_extensions = ca_extensions
string_mask = utf8only

[ ca_test_dn ]

commonName_default = Xrootd CA

[ ca_extensions ]

basicConstraints = critical,CA:true
keyUsage = keyCertSign,cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid

[ signing_policy ]
countryName            = optional
stateOrProvinceName    = optional
localityName           = optional
organizationName       = optional
organizationalUnitName = optional
commonName             = supplied
emailAddress           = optional

[ cert_extensions ]

basicConstraints = critical,CA:false
keyUsage = digitalSignature
extendedKeyUsage = critical, serverAuth, clientAuth

EOF

# Create the CA certificate
openssl req -x509 -key "$XROOTD_CONFIGDIR/tlscakey.pem" -config "$XROOTD_CONFIGDIR/tlsca.ini" -out "$XROOTD_CONFIGDIR/tlsca.pem" -outform PEM -subj "/CN=XRootD CA" 0<&- >> "$BINARY_DIR/tests/$TEST_NAME/server.log"
if [ "$?" -ne 0 ]; then
  echo "Failed to generate CA request"
  exit 1
fi

# Create the host certificate request
openssl genrsa -out "$XROOTD_CONFIGDIR/tls.key" 4096 >> "$BINARY_DIR/tests/$TEST_NAME/server.log"
openssl req -new -key "$XROOTD_CONFIGDIR/tls.key" -config "$XROOTD_CONFIGDIR/tlsca.ini" -out "$XROOTD_CONFIGDIR/tls.csr" -outform PEM -subj "/CN=localhost" 0<&- >> "$BINARY_DIR/tests/$TEST_NAME/server.log"
if [ "$?" -ne 0 ]; then
  echo "Failed to generate host certificate request"
  exit 1
fi

openssl ca -config "$XROOTD_CONFIGDIR/tlsca.ini" -batch -policy signing_policy -extensions cert_extensions -out "$XROOTD_CONFIGDIR/tls.crt" -infiles "$XROOTD_CONFIGDIR/tls.csr" 0<&- 2>> "$BINARY_DIR/tests/$TEST_NAME/server.log"
if [ "$?" -ne 0 ]; then
  echo "Failed to sign host certificate request"
  exit 1
fi


# Create xrootd configuration and runtime directory structure
XROOTD_EXPORTDIR="$RUNDIR/xrootd-export"
mkdir -p "$XROOTD_EXPORTDIR"

# XRootD has strict length limits on the admin path location.
# Therefore, we also create a directory in /tmp.
XROOTD_RUNDIR=$(mktemp -d -p /tmp xrootd_test.XXXXXXXX)

export XROOTD_CONFIG="$XROOTD_CONFIGDIR/xrootd.cfg"
cat > "$XROOTD_CONFIG" <<EOF

all.trace    all
http.trace   all
xrd.trace    all
xrootd.trace all
scitokens.trace all
httpserver.trace all dump

xrd.port any

all.export /
all.sitename  XRootD
all.adminpath $XROOTD_RUNDIR
all.pidpath   $XROOTD_RUNDIR

xrootd.seclib libXrdSec.so

ofs.authorize 1

acc.authdb $XROOTD_CONFIGDIR/authdb

xrd.protocol XrdHttp:any libXrdHttp.so
http.header2cgi Authorization authz

xrd.tlsca certfile $XROOTD_CONFIGDIR/tlsca.pem
xrd.tls $XROOTD_CONFIGDIR/tls.crt $XROOTD_CONFIGDIR/tls.key

oss.localroot $XROOTD_EXPORTDIR

EOF

cat > $XROOTD_CONFIGDIR/authdb <<EOF

u * / all

EOF

# Export some data through the origin
echo "Hello, World" > "$XROOTD_EXPORTDIR/hello_world.txt"

# Launch XRootD daemon.
"$XROOTD_BIN" -c "$XROOTD_CONFIG" -l "$BINARY_DIR/tests/$TEST_NAME/server.log" 0<&- >>"$BINARY_DIR/tests/$TEST_NAME/server.log" 2>>"$BINARY_DIR/tests/$TEST_NAME/server.log" &
XROOTD_PID=$!
echo "xrootd daemon PID: $XROOTD_PID"

echo "XRootD logs are available at $BINARY_DIR/tests/$TEST_NAME/server.log"

# Build environment file for remainder of tests
XROOTD_URL=$(grep "Xrd_ProtLoad: enabling port" "$BINARY_DIR/tests/$TEST_NAME/server.log" | grep 'for protocol XrdHttp' | awk '{print $7}')
IDX=0
while [ -z "$XROOTD_URL" ]; do
  sleep 1
  XROOTD_URL=$(grep "Xrd_ProtLoad: enabling port" "$BINARY_DIR/tests/$TEST_NAME/server.log" | grep 'for protocol XrdHttp' | awk '{print $7}')
  IDX=$(($IDX+1))
  if ! kill -0 "$XROOTD_PID" 2>/dev/null; then
    echo "xrootd process (PID $XROOTD_PID) failed to start" >&2
    exit 1
  fi
  if [ $IDX -gt 1 ]; then
    echo "Waiting for xrootd to start ($IDX seconds so far) ..."
  fi
  if [ $IDX -eq 60 ]; then
    echo "xrootd failed to start - failing"
    exit 1
  fi
done
XROOTD_URL="https://localhost:$XROOTD_URL/"
echo "xrootd started at $XROOTD_URL"

XROOTD_HTTPSERVER_CONFIG="$XROOTD_CONFIGDIR/xrootd-httpserver.cfg"
cat > "$XROOTD_HTTPSERVER_CONFIG" <<EOF

httpserver.trace all dump
httpserver.url_base $XROOTD_URL
httpserver.storage_prefix /

EOF

echo "http server config: $XROOTD_HTTPSERVER_CONFIG"

cat > "$BINARY_DIR/tests/$TEST_NAME/setup.sh" <<EOF
XROOTD_BIN=$XROOTD_BIN
XROOTD_PID=$XROOTD_PID
XROOTD_URL=$XROOTD_URL
X509_CA_FILE=$XROOTD_CONFIGDIR/tlsca.pem
XROOTD_CFG=$XROOTD_HTTPSERVER_CONFIG
EOF

echo "Test environment written to $BINARY_DIR/tests/$TEST_NAME/setup.sh"
