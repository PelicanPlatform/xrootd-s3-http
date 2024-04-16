
# S3/HTTP filesystem plugins for XRootD
These filesystem plugins for [XRootD](https://github.com/xrootd/xrootd) allow you to serve objects from S3 and HTTP backends through an XRootD server.

## Building and Installing
Assuming XRootD, CMake>=3.13 and gcc>=8 are already installed, run:

```
mkdir build
cd build
cmake ..
make

# For system installation, uncomment:
# make install
```

If building XRootD from source instead, add `-DXROOTD_DIR` to the CMake command line
to point it at the installed directory.

### Building with Tests

Unit tests for this repository require `gtest`, which for RHEL-based linux distributions can be installed with:
```bash
dnf install gtest
```

Once `gtest` is installed, the unit tests can be compiled with a slight modification to your build command:

```
mkdir build
cd build
cmake -DXROOTD_PLUGINS_BUILD_UNITTESTS=ON -DXROOTD_PLUGINS_EXTERNAL_GTEST=ON ..
make
```

This creates the directory `build/test` with two unit test executables that can be run:
- `build/test/s3-gtest`
- `build/test/http-gtest`

## Configuration

### Configure an HTTP Server Backend

To configure the HTTP server plugin, add the following line to the Xrootd configuration file:

```
ofs.osslib </path/to/libXrdHTTPServer.so>
```

Here's a minimal config file

```
# Enable the HTTP protocol on port 1094 (same as the default XRootD port)
# NOTE: This is NOT the HTTP plugin -- it is the library XRootD uses to
# speak the HTTP protocol, as opposed to the root protocol, for incoming requests
xrd.protocol http:1094 libXrdHttp.so

# Allow access to path with given prefix.
#
all.export  </exported/server/prefix>

# Setting up HTTP plugin
ofs.osslib libXrdHTTPServer.so
# Use this if libXrdHTTPServer.so is in a development directory
# ofs.osslib /path/to/libXrdHTTPServer.so

# Upon last testing, the plugin did not yet work in async mode
xrootd.async off

# Configure the upstream HTTP server that XRootD is to treat as a filesystem
httpserver.host_name <hostname of HTTP server>
httpserver.host_url <host url>
```

### Configure an S3 Backend

To configure the S3 plugin, add the following line to the Xrootd configuration file:

```
ofs.osslib </path/to/libXrdS3.so>
```

Here's a minimal config file

```
# Enable the HTTP protocol on port 1094 (same as the default XRootD port)
# The S3 plugin use
xrd.protocol http:1094 libXrdHttp.so

# Allow access to path with given prefix.
#
all.export  </exported/server/prefix>

# Setting up S3 plugin
ofs.osslib libXrdS3.so
# Use this if libXrdS3.so is in a development directory
# ofs.osslib /path/to/libXrdS3.so

# Upon last testing, the plugin did not yet work in async mode
xrootd.async off

#example url
#https://<origin url>/my-magic-path/bar/foo
# these must be in this order to allow parsing of multiple entries
# To export a bucket requiring an access/private key:
s3.begin
s3.path_name        my-magic-path
s3.bucket_name      hubzero-private-rich
s3.service_name     s3.amazonaws.com
s3.region           us-east-1
s3.access_key_file  /xrootd-dev/access-key
s3.secret_key_file  /xrootd-dev/secret-key
s3.service_url      https://s3.us-east-1.amazonaws.com
s3.end

# To export an unauthenticated (public) bucket, remove
# the key-related directives
s3.begin
s3.path_name        my-other-magic-path
s3.bucket_name      hubzero-private-rich-2
s3.service_name     s3.amazonaws.com
s3.region           us-east-1
s3.service_url      https://s3.us-east-1.amazonaws.com
s3.end

# Specify the path style for URL queries at the endpoint. Valid
# options are `path` and `virtual`, where path corresponds to URLs
# like `https://my-service-url.com/bucket/object` and virtual
# corresponds to URLs like `https://bucket.my-service-url.com/object`
s3.url_style        virtual
```


## Startup and Testing

### HTTP Server Backend

Assuming you named the config file `xrootd-http.cfg`, as a non-rootly user run:

```
xrootd -d -c xrootd-http.cfg
```

In a separate terminal, run

```
curl -v http://localhost:1094/<host name>/<URL path to object>
```

### S3 Server Backend
Startup and Testing

Assuming you named the config file `xrootd-s3.cfg`, as a non-rootly user run:

```
xrootd -d -c xrootd-s3.cfg
```

In a separate terminal, run

```
curl -v http://localhost:1094/<path name>/<object name>
```