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
any 2.5-series servers. It has not been tested against other Firebird
versions.


Installation
------------

See file `INSTALL.md`.


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
