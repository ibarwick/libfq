Summary: A wrapper library for the Firebird C API
Name: libfq
Version: 0.1.4
Release: 1
Source: libfq-%{version}.tar.gz
URL: https://github.com/ibarwick/libfq
License: PostgreSQL
Group: Development/Libraries/C and C++
Packager: Ian Barwick
BuildRequires: firebird-devel
BuildRoot: %{_tmppath}/%{name}-%{version}-build
Requires: libfbclient2

%description
A libfq wrapper library for the Firebird C API, loosely based on
libpq for PostgreSQL.

%prep
%setup
./configure --prefix=%{_prefix} --with-ibase=/usr/include/firebird --libdir=/usr/lib64/
%build

make
%install
rm -rf $RPM_BUILD_ROOT
make DESTDIR=$RPM_BUILD_ROOT install
%clean
rm -rf $RPM_BUILD_ROOT
%files
%defattr(-, root, root)
/usr/lib64/libfq.a
/usr/lib64/libfq.la
/usr/lib64/libfq.so
/usr/lib64/libfq.so.0
/usr/lib64/libfq.so.%version
/usr/include/libfq-expbuffer.h
/usr/include/libfq-int.h
/usr/include/libfq.h

%changelog
* Tue Feb 11 2014 Ian Barwick (barwick@gmail.com)
- libfq 0.1.4
* Sun Feb 2 2014 Ian Barwick (barwick@gmail.com)
- First draft
