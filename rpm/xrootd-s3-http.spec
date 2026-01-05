Name:		xrootd-s3-http
Version:        0.6.0
Release:        1%{?dist}
Summary:        S3/HTTP/Globus filesystem plugins for xrootd

License:        Apache-2.0
URL:            https://github.com/PelicanPlatform/%{name}
Source0:        %{url}/archive/refs/tags/v%{version}/%{name}-%{version}.tar.gz

%define xrootd_current_major 5
%define xrootd_current_minor 7
%define xrootd_next_major 6

BuildRequires: cmake3
BuildRequires: gcc-c++
BuildRequires: make
BuildRequires: xrootd-server-libs >= 1:%{xrootd_current_major}
BuildRequires: xrootd-server-libs <  1:%{xrootd_next_major}
BuildRequires: xrootd-server-devel >= 1:%{xrootd_current_major}
BuildRequires: xrootd-server-devel <  1:%{xrootd_next_major}
BuildRequires: libcurl-devel
BuildRequires: openssl-devel
BuildRequires: tinyxml2-devel
BuildRequires: nlohmann-json-devel

Requires: xrootd-server >= 1:%{xrootd_current_major}.%{xrootd_current_minor}
Requires: xrootd-server <  1:%{xrootd_next_major}.0.0-1

%description
%{summary}

%prep
%setup -q

%build
%cmake . -DCMAKE_BUILD_TYPE=RelWithDebInfo -DXROOTD_EXTERNAL_TINYXML2=ON
cmake --build redhat-linux-build --verbose

%install
%cmake_install

%files
%{_libdir}/libXrdPelicanHttpCore-5.so
%{_libdir}/libXrdHTTPServer-5.so
%{_libdir}/libXrdS3-5.so
%{_libdir}/libXrdOssHttp-5.so
%{_libdir}/libXrdOssGlobus-5.so
%{_libdir}/libXrdOssS3-5.so
%{_libdir}/libXrdOssFilter-5.so
%{_libdir}/libXrdOssPosc-5.so
%doc README.md
%license LICENSE

%changelog
* Fri Jan 05 2026 Patrick Brophy <patrick.brophy2@gmail.com> - 0.6.0-1
- Fix some race conditions with the Globus plugin.
- Package the new libXrdPelicanHttpCore shared object which addresses the above.
- Add file size verification to POSC plugin to prevent incomplete uploads

* Fri Oct 24 2025 Brian Bockelman <bbockelman@morgridge.org> - 0.5.3-1
- Fix directory listings for the POSC filtering plugin.

* Thu Oct 2 2025 Justin Hiemstra <jhiemstra@wisc.edu> - 0.5.2-1
- Fix download buffers that could try reading past the end of the file

* Wed Aug 27 2025 Brian Bockelman <bbockelman@morgridge.org> - 0.5.1-1
- Minor build fixes for a wider set of platforms.
- Ensure unit tests can succeed even when running without network connectivity
- Bump user agent version (forgotten in 0.5.0).

* Wed Jul 30 2025 William Jiang <whjiang@wisc.edu> - 0.5.0-1
- Add support for Globus endpoints (reads, writes, listings)
- Add support for HTTP writes
- Add new "Persist on Successful Close" (POSC) plugin to prevent files being
  uploaded from being visible in the namespace.
- The HTTP OSS plugin can do file listings based on generated directories.

* Fri May 30 2025 Brian Bockelman <bbockelman@morgridge.org> - 0.4.1-1
- Fix stall timeouts which would never fire.
- Fix bug where S3 rate limiting would result in corrupt data being sent back to the client.
- Remove redundant HEAD which was invoked twice on S3 file open.
- Put libcurl into threadsafe mode, avoiding potential deadlocks or long unresponsive periods.

* Thu May 29 2025 Brian Bockelman <bbockelman@morgridge.org> - 0.4.0-1
- Improve logging messages to include timing of read requests
- Implement the vector read method, used by some clients.
- Send basic cache performance statistics out via the XRootD OSS g-stream.

* Sat Mar 15 2025 Brian Bockelman <bbockelman@morgridge.org> - 0.3.0-1
- Add new filter plugin to the package
- Add renamed plugins to the package

* Sat Feb 1 2025 Brian Bockelman <bbockelman@morgridge.org> - 0.2.1-1
- Bump to upstream version 0.2.1.

* Tue Nov 28 2023 Justin Hiemstra <jhiemstra@wisc.edu> - 0.0.2-1
- Add HTTPServer plugin

* Tue Dec 06 2022 Brian Bockelman <bbockelman@morgridge.org> - 0.0.1-1
- Initial, "Hello world" version of the S3 filesystem plugin
