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

`libfq` should work on any reasonably POSIX-like system; see the
[INSTALL.md](INSTALL.md) file for details.

It has been developed against Firebird 2.5 and Firebird 3, and should
work with Firebird 2.1 and later. It supports the `BOOLEAN` datatype
introduced in Firebird 3.


Installation
------------

RPM packages for CentOS and derivatives, and current Fedora versions, are
available via the Fedora "copr" build system; for details see here:
<https://copr.fedorainfracloud.org/coprs/ibarwick/libfq/>

For installation from source, see file [INSTALL.md](INSTALL.md).


Applications
------------

Following applications make use of `libfq`:

- [firebird_fdw](https://github.com/ibarwick/firebird_fdw) - a foreign data wrapper for PostgreSQL
- [fbsql](https://github.com/ibarwick/fbsql) - a command-line client inspired by PostgreSQL's `psql`


KNOWN ISSUES
------------

* Data types
  - `BLOB` (other than `SUB_TYPE TEXT`) not supported
  - `ARRAY` datatype currently not handled

* Parameterized queries (function `FQexecParams()`):
  - `NUMERIC`/`DECIMAL`: may overflow if more digits than permitted are supplied
