
S3 filesystem plugin for XRootD
================================

This filesystem plugin is intended to demonstrate the ability to have a S3 bucket as an
underlying "filesystem" for an XRootD server

It is currently quite experimental, aiming to show a "minimum viable product".

Configuration
-------------

To configure the plugin, add the following line to the Xrootd configuration file:

```
ofs.osslib libXrdS3.so
```

Here's a minimal config file

```
###########################################################################
# This is a very simple sample configuration file sufficient to start an  #
# xrootd data server using the default port 1094, exporting both the      #
# xrootd and HTTP protocols, and enabling the S3 plugin                   #
#                                                                         #
###########################################################################

# The adminpath and pidpath variables indicate where the pid and various
# IPC files should be placed.  These can be placed in /tmp for a developer-
# quality server.
#
all.adminpath /var/spool/xrootd
all.pidpath /run/xrootd

# Enable the HTTP protocol on port 1094 (same as the default XRootD port):
xrd.protocol http:1094 libXrdHttp.so

# This exports global without authentication the prefix /aws/us-east-1
all.export /aws/us-east-1

# Use this if libXrdS3-5.so is in a standard search path
# Note that XRootD will automatically inject the '-5' into the filename
ofs.osslib libXrdS3.so

## Use this if you have it in a developer directory
#
# ofs.osslib /home/myself/xrootd-s3-build/release_dir/lib/libXrdS3.so
```

Startup and Testing
-------------------

Assuming you named the config file `xrootd-s3.cfg`, run:

```
xrootd -d -c xrootd-s3.cfg
```

In a separate terminal, run

```
curl -v http://localhost:1094/aws/us-east-1/bucket/hello_world
```
