libfq - C API wrapper for Firebird
==================================

libfq provides a number of functions which act as convenience
wrappers around the native Firebird C API, which is cumbersome to use.

It is loosely based on PostgreSQL's libpq and provides a subset of
that API's functions - names beginning with "FQ" rather than "PQ"
of course - with more-or-less identical function signatures.
It also provides a small number of Firebird-specific functions.

While basically functional, libfq is still work-in-progress and
the API definitions may change in an incompatible way.
USE AT YOUR OWN RISK.


Requirements and compatibility
------------------------------

libfq should work on any UNIX-like system; see the INSTALL file for
details.

It has been developed against Firebird 2.5.2, and should work with
any 2.5-series servers. It has not been tested against other Firebird
versions.


KNOWN ISSUES
------------

* Compatiblity:
  - tested on Linux and OS X
  - should work on other reasonably UNIX-like systems
  - unlikely to work without modification on Win32 systems

* Data types
  - BLOB and ARRAY datatypes are not handled

* Parameterized queries (function "FQexecParams()"):
  - TIMESTAMP/TIME: currently sub-second units are truncated
  - NUMERIC/DECIMAL: may overflow if more digits than
       permitted are supplied

* Character sets, encoding and wide characters
  - all data is assumed to be UTF8
  - however there is currently no special handling of non-ASCII
     characters
