
S3/HTTP filesystem plugins for XRootD
================================

These filesystem plugins are intended to demonstrate the ability to have an S3 bucket
or raw HTTP server as an underlying "filesystem" for an XRootD server.

They are currently quite experimental, aiming to show a "minimum viable product".

Building and Installing
-----------------------
Assuming XRootD, CMake>=3.13 and gcc>=8 are already installed, run:

```
mkdir build
cd build
cmake ..
make install
```


If building XRootD from source instead, add `-DXROOTD_DIR` to the CMake command line
to point it at the installed directory.

Configuration
-------------

## Configure an HTTP Server Backend

To configure the HTTP server plugin, add the following line to the Xrootd configuration file:

```
ofs.osslib </path/to/libXrdHTTPServer.so>
```

Here's a minimal config file

```
###########################################################################
# This is a very simple sample configuration file sufficient to start an  #
# xrootd file caching proxy server using the default port 1094. This      #
# server runs by itself (stand-alone) and does not assume it is part of a #
# cluster. You can then connect to this server to access files in '/tmp'. #
# Consult the the reference manuals on how to create more complicated     #
# configurations.                                                         #
#                                                                         #
# On successful start-up you will see 'initialization completed' in the   #
# last message. You can now connect to the xrootd server.                 #
#                                                                         #
# Note: You should always create a *single* configuration file for all    #
# daemons related to xrootd.                                              #
###########################################################################

# The adminpath and pidpath variables indicate where the pid and various
# IPC files should be placed.  These can be placed in /tmp for a developer-
# quality server.
#
all.adminpath /var/spool/xrootd
all.pidpath /run/xrootd

# Enable the HTTP protocol on port 1094 (same as the default XRootD port):
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


## Configure an S3 Backend

To configure the S3 plugin, add the following line to the Xrootd configuration file:

```
ofs.osslib </path/to/libXrdS3.so>
```

Here's a minimal config file

```
###########################################################################
# This is a very simple sample configuration file sufficient to start an  #
# xrootd file caching proxy server using the default port 1094. This      #
# server runs by itself (stand-alone) and does not assume it is part of a #
# cluster. You can then connect to this server to access files in '/tmp'. #
# Consult the the reference manuals on how to create more complicated     #
# configurations.                                                         #
#                                                                         #
# On successful start-up you will see 'initialization completed' in the   #
# last message. You can now connect to the xrootd server.                 #
#                                                                         #
# Note: You should always create a *single* configuration file for all    #
# daemons related to xrootd.                                              #
###########################################################################

# The adminpath and pidpath variables indicate where the pid and various
# IPC files should be placed.  These can be placed in /tmp for a developer-
# quality server.
#
all.adminpath /var/spool/xrootd
all.pidpath /run/xrootd

# Enable the HTTP protocol on port 1094 (same as the default XRootD port):
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
s3.begin
s3.path_name        my-magic-path
s3.bucket_name      hubzero-private-rich
s3.service_name     s3.amazonaws.com
s3.region           us-east-1
s3.access_key_file  /xrootd-dev/access-key
s3.secret_key_file  /xrootd-dev/secret-key
s3.service_url      https://s3.us-east-1.amazonaws.com
s3.end

s3.begin
s3.path_name        my-other-magic-path
s3.bucket_name      hubzero-private-rich-2
s3.service_name     s3.amazonaws.com
s3.region           us-east-1
s3.access_key_file  /xrootd-dev/access-key-2
s3.secret_key_file  /xrootd-dev/secret-key-2
s3.service_url      https://s3.us-east-1.amazonaws.com
s3.end

s3.url_style        virtual
```


Startup and Testing
-------------------

## HTTP Server Backend

Assuming you named the config file `xrootd-http.cfg`, as a non-rootly user run:

```
xrootd -d -c xrootd-http.cfg
```

In a separate terminal, run

```
curl -v http://localhost:1094/<host name>/<URL path to object>
```

## S3 Server Backend
Startup and Testing
-------------------

## HTTP Server Backend

Assuming you named the config file `xrootd-s3.cfg`, as a non-rootly user run:

```
xrootd -d -c xrootd-s3.cfg
```

In a separate terminal, run

```
curl -v http://localhost:1094/<service_name>/<region>/<path to bucket/object>
```