#!/bin/sh
#
# Setup script for the deadlock-detection integration test.
#
# Brings up an xrootd server whose OSS stack is
#   XrdOssDeadlock (monitor)  ->  XrdOssDummyStall (stalls Stat on "stall"
#   paths)  ->  default OSS
# and whose authorization stack is
#   XrdAccDeadlock (monitor)  ->  authdb-based default authorization.
# This exercises both the OSS and the authorization wrappers at once.

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

echo "Setting up xrootd for the $TEST_NAME deadlock test"

XROOTD_BIN="$XROOTD_BINDIR/xrootd"
if [ ! -x "$XROOTD_BIN" ]; then
  echo "xrootd binary not found at $XROOTD_BIN; cannot run test"
  exit 1
fi

mkdir -p "$BINARY_DIR/tests/$TEST_NAME"
RUNDIR=$(mktemp -d -p "$BINARY_DIR/tests/$TEST_NAME" test_run.XXXXXXXX)
if [ ! -d "$RUNDIR" ]; then
  echo "Failed to create test run directory; cannot run xrootd"
  exit 1
fi
echo "Using $RUNDIR as the test run's home directory."
cd "$RUNDIR" || exit 1

export XROOTD_CONFIGDIR="$RUNDIR/xrootd-config"
mkdir -p "$XROOTD_CONFIGDIR/ca"
SERVER_LOG="$BINARY_DIR/tests/$TEST_NAME/server.log"
echo > "$SERVER_LOG"

# Create the TLS credentials for the test (XrdHttp requires TLS to be set up
# even when we speak plain HTTP to it).
openssl genrsa -out "$XROOTD_CONFIGDIR/tlscakey.pem" 4096 >> "$SERVER_LOG" 2>&1
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

openssl req -x509 -key "$XROOTD_CONFIGDIR/tlscakey.pem" -config "$XROOTD_CONFIGDIR/tlsca.ini" -out "$XROOTD_CONFIGDIR/tlsca.pem" -outform PEM -subj "/CN=XRootD CA" 0<&- >> "$SERVER_LOG" 2>&1 || { echo "Failed to generate CA"; exit 1; }
openssl genrsa -out "$XROOTD_CONFIGDIR/tls.key" 4096 >> "$SERVER_LOG" 2>&1
openssl req -new -key "$XROOTD_CONFIGDIR/tls.key" -config "$XROOTD_CONFIGDIR/tlsca.ini" -out "$XROOTD_CONFIGDIR/tls.csr" -outform PEM -subj "/CN=localhost" 0<&- >> "$SERVER_LOG" 2>&1 || { echo "Failed to generate host CSR"; exit 1; }
openssl ca -config "$XROOTD_CONFIGDIR/tlsca.ini" -batch -policy signing_policy -extensions cert_extensions -out "$XROOTD_CONFIGDIR/tls.crt" -infiles "$XROOTD_CONFIGDIR/tls.csr" 0<&- >> "$SERVER_LOG" 2>&1 || { echo "Failed to sign host cert"; exit 1; }

XROOTD_EXPORTDIR="$RUNDIR/xrootd-export"
mkdir -p "$XROOTD_EXPORTDIR"
XROOTD_RUNDIR=$(mktemp -d -p /tmp xrootd_deadlock.XXXXXXXX)

# A normal file (used by the positive test) and a file whose name contains
# "stall" (used by the negative test to trip the DummyStall Stat handler).
echo "Hello, World" > "$XROOTD_EXPORTDIR/hello_world.txt"
echo "Hello, World" > "$XROOTD_EXPORTDIR/stall_me.txt"

DEADLOCK_LOG="$BINARY_DIR/tests/$TEST_NAME/deadlocks.log"

export XROOTD_CONFIG="$XROOTD_CONFIGDIR/xrootd.cfg"
cat > "$XROOTD_CONFIG" <<EOF
all.trace    all
http.trace   all
xrd.trace    all
xrootd.trace all

xrd.port any
all.export /
all.sitename  XRootD
all.adminpath $XROOTD_RUNDIR
all.pidpath   $XROOTD_RUNDIR

xrootd.seclib libXrdSec.so
ofs.authorize 1
acc.authdb $XROOTD_CONFIGDIR/authdb
# Stack the deadlock-detection authorization wrapper on top of the default
# authdb-based authorization.
ofs.authlib ++ $BINARY_DIR/libXrdAccDeadlock.so

xrd.protocol XrdHttp:any libXrdHttp.so
http.header2cgi Authorization authz

xrd.tlsca certfile $XROOTD_CONFIGDIR/tlsca.pem
xrd.tls $XROOTD_CONFIGDIR/tls.crt $XROOTD_CONFIGDIR/tls.key

oss.localroot $XROOTD_EXPORTDIR
# OSS stack (innermost first): default OSS <- DummyStall <- Deadlock monitor.
ofs.osslib ++ $BINARY_DIR/libXrdOssDummyStall.so
ofs.osslib ++ $BINARY_DIR/libXrdOssDeadlock.so

# Short timeout so the negative test does not have to wait long.
deadlock.timeout 3
deadlock.logfile $DEADLOCK_LOG
EOF

cat > "$XROOTD_CONFIGDIR/authdb" <<EOF
u * / all
EOF

ASAN_OPTIONS=detect_odr_violation=0 "$XROOTD_BIN" -c "$XROOTD_CONFIG" -l "$SERVER_LOG" 0<&- >>"$SERVER_LOG" 2>>"$SERVER_LOG" &
XROOTD_PID=$!
echo "xrootd daemon PID: $XROOTD_PID"

# Discover the port XrdHttp bound to.
XROOTD_PORT=$(grep "Xrd_ProtLoad: enabling port" "$SERVER_LOG" | grep 'for protocol XrdHttp' | awk '{print $7}')
IDX=0
while [ -z "$XROOTD_PORT" ]; do
  sleep 1
  XROOTD_PORT=$(grep "Xrd_ProtLoad: enabling port" "$SERVER_LOG" | grep 'for protocol XrdHttp' | awk '{print $7}')
  IDX=$((IDX+1))
  if ! kill -0 "$XROOTD_PID" 2>/dev/null; then
    echo "xrootd process (PID $XROOTD_PID) failed to start" >&2
    cat "$SERVER_LOG"
    exit 1
  fi
  if [ $IDX -ge 30 ]; then
    echo "xrootd failed to start within 30s - failing"
    cat "$SERVER_LOG"
    exit 1
  fi
done

XROOTD_URL="https://localhost:$XROOTD_PORT/"
echo "xrootd started at $XROOTD_URL"

cat > "$BINARY_DIR/tests/$TEST_NAME/setup.sh" <<EOF
XROOTD_PID=$XROOTD_PID
XROOTD_URL=$XROOTD_URL
X509_CA_FILE=$XROOTD_CONFIGDIR/tlsca.pem
DEADLOCK_LOG=$DEADLOCK_LOG
SERVER_LOG=$SERVER_LOG
EOF

echo "Test environment written to $BINARY_DIR/tests/$TEST_NAME/setup.sh"
