# Pelican filesystem plugins for XRootD
This repository contains various plugins for [XRootD](https://github.com/xrootd/xrootd)'s "Open Storage System" (OSS) layer,
which affects how XRootD serves objects from storage.

The plugins in the repository include:
- `XrdOssHttp`: Exposes a backend HTTP(S) server as a backend storage.
- `XrdOssS3`: Exposes a backend S3-compatible interface as a backend storage.
- `XrdOssFilter`: A "stacking" plugin (meant to be loaded on top of another OSS plugin)
  that exports only files/directories matching a given list of Unix globs.
- `XrdOssPosc`: A "stacking" plugin that causes in-progress files being written from being
  visible to the filesystem and to be deleted if not successfully closed.  Here, "POSC"
  stands for "Persist on Successful Close" and is similar in spirit to the POSC functionality
  in the core XRootD (with the addition of making in-progress files not-visible in the namespace).
- `XrdN2NPrefix`: A Name2Name (N2N) plugin that performs path prefix substitution,
  allowing logical paths to be mapped to different physical paths on disk.
- `XrdOssDeadlock`: A "stacking" plugin that monitors all OSS operations for deadlocks,
  killing the process if any operation exceeds a configurable timeout threshold.
- `XrdAccDeadlock`: An authorization plugin wrapper that monitors all authorization
  operations for deadlocks.


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

Dependency packages (tinyxml, gtest, and minio/mc for tests) are automatically downloaded
if they are not present in the build environment.

### Building with Tests

Unit tests for this repository require `gtest`, which is included as a submodule of this repo. The tests can be compiled with a slight modification to your build command:

```
mkdir build
cd build
cmake -DENABLE_TESTS=ON ..
make
```

To run the test framework, execute `ctest` from the build directory:

```
ctest
```

## Configuration

### Configure an HTTP Server Backend

To configure the HTTP server plugin, add the following line to the Xrootd configuration file:

```
ofs.osslib libXrdOssHttp.so
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
ofs.osslib libXrdOssHttp.so
# Use this if libXrdOssHttp.so is in a development directory
# ofs.osslib /path/to/libXrdOssHttp.so

# Upon last testing, the plugin did not yet work in async mode
xrootd.async off

# Configure the upstream HTTP server that XRootD is to treat as a filesystem
httpserver.host_name <hostname of HTTP server>
httpserver.host_url <host url>
```

### Configure an S3 Backend

To configure the S3 plugin, add the following line to the Xrootd configuration file:

```
ofs.osslib libXrdOssS3.so
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
ofs.osslib libXrdOssS3.so
# Use this if libXrdOssS3.so is in a development directory
# ofs.osslib /path/to/libXrdOssS3.so

# The plugin does not support plugin mode
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
s3.url_style        path
s3.end

# To export an unauthenticated (public) bucket, remove
# the key-related directives
s3.begin
s3.path_name        my-other-magic-path
s3.bucket_name      hubzero-private-rich-2
s3.service_name     s3.amazonaws.com
s3.region           us-east-1
s3.service_url      https://s3.us-east-1.amazonaws.com
s3.url_style        virtual
s3.end

# Specify the path style for URL queries at the endpoint. Valid
# options are `path` and `virtual`, where path corresponds to URLs
# like `https://my-service-url.com/bucket/object` and virtual
# corresponds to URLs like `https://bucket.my-service-url.com/object`
s3.url_style        virtual

# trace levels are
# error
# warning
# info
# debug
# dump
# debug produces a fair amount of log,
# but dump produces the actual wire traffic to the client and
# should only be used if you have a reason to do so
s3.trace debug

```

### Configure the filter plugin

The filter plugin allows you to provide multiple globs to export specific
files and directories inside a filesystem (allowing some to be made inaccessible).

Note this only affects the namespace seen by XRootD.  A user that can create symlinks
on the filesystem outside XRootD can expose hidden parts of the namespace via a specially
crafted symlink.

To load, invoke `ofs.osslib` in "stacking" mode:

```
ofs.osslib ++ libXrdOssFilter.so
```

(an absolute path may be given if `libXrdOssFilter-5.so` does not reside in a system directory)

There are three configuration commands for the filter module:

```
filter.trace [all|error|warning|info|debug|none]
filter.glob [-a] glob1 [glob2] [...]
filter.prefix prefix1 [prefix2] [...]
```

 - `filter.trace`: Controls the logging verbosity of the module.  Can be specified multiple times
   (values are additive) and multiple values can be given per line.  Example:

   ```
   filter.trace info
   ```

   The default level is `warning`.
 - `filter.glob`: Controls the visibility of files in the storage; only files matching one of the
   configured globs or prefixes can be accessed.  Can be specified multiple times (values are additive)
   and multiple globs can be given per line.

   The `-a` flag indicates that a wildcard should match all filenames, including those prefixed with a
   `.` character.  Otherwise, such "dotfiles" are not visible.

   The glob language supported by the platform's `fnmatch` C runtime function is used; additionally, the
   globstar operator (`**`) matches any names in zero-or-more directory hierarchies

   Example:

   ```
   filter.glob /foo/*/*.txt /bar/**/*.csv
   ```

   With the above configuration, the files `/foo/1/test.txt` and `/bar/2/3/data.csv` would be visible
   but the files `/foo/4/5/test.txt` and `/bar/test.txt` would not.
 - `filter.prefix`: Controls the prefixes exported.  Every file or directory under the provided prefix
   will be visible.  Can be specified multiple times (values are additive) and multiple globs can be
   given per line.

   Example:

   ```
   filter.prefix /foo /bar
   ```

   The above example would be equivalent to setting:

   ```
   filter.glob -a /foo/** /bar/**
   ```

### Configure the POSC (Persist on Successful Close) plugin

The POSC plugin allows you to make files being uploaded into the storage invisible
from readers until they have been successfully closed.  If the writer never calls
`close()` or the server crashes mid-transfer, the files will be deleted.

Note the visibility only affects the namespace seen by XRootD.  A user that can create symlinks
on the filesystem outside XRootD can expose the in-progress files if desired (but may not be
able to read them, depending on the filesystem permissions set).

To load, invoke `ofs.osslib` in "stacking" mode:

```
ofs.osslib ++ libXrdOssPosc.so
```

(an absolute path may be given if `libXrdOssPosc-5.so` does not reside in a system directory)

There are three configuration commands for the POSC module:

```
posc.trace [all|error|warning|info|debug|none]
posc.prefix posc_directory
```

 - `posc.trace`: Controls the logging verbosity of the module.  Can be specified multiple times
   (values are additive) and multiple values can be given per line.  Example:

   ```
   filter.trace info
   ```

   The default level is `warning`.

 - `posc.prefix`: Controls the directory where in-progress files will be written.  Once completed,
   they will be renamed into the final location.  If this is not specified, the module will fail
   to start; if specified multiple times, the last value wins.  Example:

   ```
   posc.prefix /in-progress
   ```

   The `posc.prefix` directory is not exported into the namespace; users will not be able to list
   its contents.

Files in the `posc.prefix` directory will have the following structure:

```
/$(posc.prefix)/$(username)/in-progress.$(timestamp).$(random)
```

where `$(username)` is the username as determined by the security framework (`anonymous` if unset).
The user directory and files will be created with the user's credential as possible.  If using
the multi-user plugin, this the `posc.prefix` directory should be world writable with the "sticky bit"
set to allow any user to create a subdirectory (but not allow users to delete other directories).

XRootD will never perform filesystem operations as root; it will not create as root and "chown" to the
user.

World-writable directories are difficult to manage.  Another option would be for the filesystem owner
to pre-create all the potential user temporary directories.

### Configure the N2N Prefix plugin

The N2N (Name2Name) prefix plugin provides path prefix substitution, allowing logical paths
to be mapped to different physical paths. This is useful when you want to expose files under
a different namespace than where they physically reside.

For example, if files are stored under `/data/physics/` but should be accessed via `/store/`,
the N2N plugin can translate `/store/file.txt` to `/data/physics/file.txt`.

To load the plugin, use the `oss.namelib` directive:

```
oss.namelib libXrdN2NPrefix.so
```

(an absolute path may be given if `libXrdN2NPrefix-5.so` does not reside in a system directory)

There is one configuration directive for the N2N prefix module:

```
prefixn2n.rule [-strict] <match_prefix> <substitute_prefix>
```

 - `prefixn2n.rule`: Defines a prefix substitution rule. The `<match_prefix>` is the logical
   path prefix that will be matched, and `<substitute_prefix>` is what it will be replaced with.
   Rules are evaluated in order; the first matching rule is applied.

   Path matching is done at path boundaries, not as string prefixes. This means `/foo` will
   match `/foo` and `/foo/bar` but NOT `/foobar`.

   The optional `-strict` flag preserves consecutive slashes (`//`) exactly as they appear.
   Without this flag (the default), consecutive slashes are normalized to single slashes.

   Examples:

   ```
   # Map /store/* to /data/cms/*
   prefixn2n.rule /store /data/cms

   # Map /cache/* to /tmp/cache/* with strict slash handling
   prefixn2n.rule -strict /cache /tmp/cache
   ```

   For paths containing spaces, use JSON-style quoted strings:

   ```
   prefixn2n.rule "/path with spaces" "/destination with spaces"
   ```

   Multiple rules can be specified:

   ```
   prefixn2n.rule /store/mc /data/monte-carlo
   prefixn2n.rule /store/data /data/physics
   prefixn2n.rule /store /data/cms
   ```

   In the above example, `/store/mc/file.txt` maps to `/data/monte-carlo/file.txt`,
   `/store/data/file.txt` maps to `/data/physics/file.txt`, and `/store/other/file.txt`
   maps to `/data/cms/other/file.txt`.

The plugin supports bidirectional mapping: `lfn2pfn` (logical to physical) applies rules
forward, while `pfn2lfn` (physical to logical) applies them in reverse.

**Note**: When used with `oss.localroot`, the N2N plugin automatically prepends the localroot
to physical paths returned by `lfn2pfn()`.

### Configure the Deadlock Detection plugin

The deadlock detection plugin monitors all filesystem or authorization operations and kills the
XRootD process with `SIGKILL` if any operation blocks for longer than a configurable timeout.
This is useful for detecting and preventing server hangs caused by deadlocks in storage backends
or authorization systems.

#### OSS Wrapper

To load the OSS wrapper, invoke `ofs.osslib` in "stacking" mode:

```
ofs.osslib ++ libXrdOssDeadlock.so
```

(an absolute path may be given if `libXrdOssDeadlock-5.so` does not reside in a system directory)

#### Authorization Wrapper

To load the authorization wrapper, use the `acc.authlib` directive with the wrapped
authorization plugin specified as a parameter:

```
acc.authlib libXrdAccDeadlock.so <wrapped_auth_plugin>
```

For example, to wrap the default XRootD authorization:

```
acc.authlib libXrdAccDeadlock.so libXrdAcc.so
```

#### Configuration Directives

There are two configuration commands for the deadlock detection module:

```
deadlock.timeout <seconds>
deadlock.logfile <path>
```

 - `deadlock.timeout`: Sets the timeout threshold in seconds. If any operation takes longer
   than this threshold, it is considered a deadlock and the process is killed. Default is
   300 seconds (5 minutes). Example:

   ```
   deadlock.timeout 180
   ```

 - `deadlock.logfile`: Optional path to a log file where deadlock events are recorded before
   the process is killed. The log file is written atomically using `open()` with `O_APPEND`
   and contains timestamps and operation names. Example:

   ```
   deadlock.logfile /var/log/xrootd/deadlocks.log
   ```

#### Example Configuration

Here's a complete example that combines deadlock detection with S3 storage:

```
# Enable HTTP protocol
xrd.protocol http:1094 libXrdHttp.so

# Export path
all.export /data

# Stack deadlock detection on top of S3 storage
ofs.osslib ++ libXrdOssDeadlock.so
ofs.osslib libXrdOssS3.so

# Configure deadlock detection
deadlock.timeout 120
deadlock.logfile /var/log/xrootd/deadlocks.log

# S3 configuration
s3.begin
s3.path_name data
s3.bucket_name my-bucket
s3.service_url https://s3.amazonaws.com
s3.region us-east-1
s3.end
```

In this configuration, any S3 operation that takes longer than 2 minutes will be logged and
the process will be killed, preventing indefinite hangs.

## Startup and Testing

Assuming you named the config file `xrootd-http.cfg`, as a non-rootly user run:

```
xrootd -d -c xrootd-http.cfg
```

In a separate terminal, run

```
curl -v http://localhost:1094/<host name>/<URL path to object>
```
