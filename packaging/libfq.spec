Summary: A wrapper library for the Firebirds C API
Name: libfq
Version: 0.1.2
Release: 1
Source: libfq-0.1.2.tar.gz
URL: https://github.com/ibarwick/libfq
License: PostgreSQL
Group: Development/Libraries/C and C++
Packager: Ian Barwick
BuildRequires: firebird-devel
BuildRoot:      %{_tmppath}/%{name}-%{version}-build

%description
A libfq wrapper library for the Firebird C API, loosely based on
libpq for PostgreSQL.

%prep
%setup
./configure --prefix=%{_prefix} --with-ibase=/usr/include/firebird
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
* Sun Feb 2 2014 Ian Barwick (barwick@gmail.com)
- First draft
