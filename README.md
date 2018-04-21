libfq - C API wrapper for Firebird
==================================

`libfq` provides a number of functions which act as convenience
wrappers around the native Firebird C API, which is cumbersome to use.

It is loosely based on PostgreSQL's `libpq` and provides a subset of
that API's functions - names beginning with `FQ` rather than `PQ`
of course - with more-or-less identical function signatures.
It also provides a small number of Firebird-specific functions.

While basically functional, `libfq` is still work-in-progress and
the API definitions may change in an incompatible way.
*USE AT YOUR OWN RISK*.


Requirements and compatibility
------------------------------

`libfq` should work on any reasonably POSIX-like system; see the `INSTALL.md`
file for details.

It has been developed against Firebird 2.5.7, and should work with
any 2.5-series servers. A quick test has shown it works with Firebird 3,
but as yet no susbtantial testing has been carried out. It has not been
tested against Firebird 2.1 or earlier.

Installation
------------

RPM packages for CentOS and derivatives, and current Fedora versions, are
available via the Fedora "copr" build system; for details see here:
<https://copr.fedorainfracloud.org/coprs/ibarwick/libfq/>

For installation from source, see file [INSTALL.md](INSTALL.md).


KNOWN ISSUES
------------

* Compatibility:
  - tested on Linux and OS X
  - should work on other reasonably POSIX-compatible systems
  - untested on Windows

* Data types
  - `BLOB` and `ARRAY` datatypes are currently not handled

* Parameterized queries (function `FQexecParams()`):
  - `NUMERIC`/`DECIMAL`: may overflow if more digits than permitted are supplied
