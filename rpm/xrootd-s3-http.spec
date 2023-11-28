Name: xrootd-s3-http
Version: 0.0.2
Release: 1%{?dist}
Summary: S3/HTTP filesystem plugins for xrootd

Group: System Environment/Daemons
License: BSD
# Generated from:
# git archive v%{version} --prefix=xrootd-s3-http-%{version}/ | gzip -7 > ~/rpmbuild/SOURCES/xrootd-s3-http-%{version}.tar.gz
URL: https://github.com/pelicanplatform/xrootd-s3-http
Source0: %{name}-%{version}.tar.gz

%define xrootd_current_major 5
%define xrootd_current_minor 5
%define xrootd_next_major 6

BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
BuildRequires: xrootd-server-libs >= 1:%{xrootd_current_major}
BuildRequires: xrootd-server-libs <  1:%{xrootd_next_major}
BuildRequires: xrootd-server-devel >= 1:%{xrootd_current_major}
BuildRequires: xrootd-server-devel <  1:%{xrootd_next_major}
BuildRequires: cmake
BuildRequires: gcc-c++
BuildRequires: libcurl-devel
BuildRequires: openssl-devel

Requires: xrootd-server >= 1:%{xrootd_current_major}.%{xrootd_current_minor}
Requires: xrootd-server <  1:%{xrootd_next_major}.0.0-1

%description
%{summary}

%prep
%setup -q

%build
%cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .
make VERBOSE=1 %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%{_libdir}/libXrdS3*.so
%{_libdir}/libXrdHTTPServer*.so

%changelog
* Tue Nov 28 2023 Justin Hiemstra <jhiemstra@wisc.edu> - 0.0.2-1
- Add HTTPServer plugin

* Tue Dec 06 2022 Brian Bockelman <bbockelman@morgridge.org> - 0.0.1-1
- Initial, "Hello world" version of the S3 filesystem plugin

