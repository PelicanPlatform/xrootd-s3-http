Name:		xrootd-s3-http
Version:        0.2.0
Release:        1%{?dist}
Summary:        S3/HTTP filesystem plugins for xrootd

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

Requires: xrootd-server >= 1:%{xrootd_current_major}.%{xrootd_current_minor}
Requires: xrootd-server <  1:%{xrootd_next_major}.0.0-1

%description
%{summary}

%prep
%setup -q

%build
%cmake .
%cmake_build

%install
%cmake_install

%files
%{_libdir}/libXrdHTTPServer-5.so
%{_libdir}/libXrdS3-5.so
%doc README.md
%license LICENSE

%changelog
* Sat Feb 1 2025 Brian Bockelman <bbockelman@morgridge.org> - 0.2.0-1
- Bump to upstream version 0.2.1.

* Tue Nov 28 2023 Justin Hiemstra <jhiemstra@wisc.edu> - 0.0.2-1
- Add HTTPServer plugin

* Tue Dec 06 2022 Brian Bockelman <bbockelman@morgridge.org> - 0.0.1-1
- Initial, "Hello world" version of the S3 filesystem plugin
