Installing libfq from source
============================

`libfq` should work on any UNIX-like system. Note that it requires
the Firebird header and client library which may be installed
as part of the Firebird binary package (e.g. on OS X), or which
need to be installed as separate packages (e.g. `firebird-devel`).

`libfq` can be installed from source in the usual way:

    ./configure
    make
    make install

The standard configuration options can be specified.

Additionally two custom options are available

- `--with-ibase=DIR`: location of Firebird's `ibase.h` header file, which
  is often in a non-standard location (see below)
- `--with-fbclient=DIR`: location of the `fbclient` library, if not in
  standard library path (e.g. `/usr/local/lib`)


Known ibase.h locations:
------------------------

OpenSuSE, CentOS/RedHat:

    /usr/include/firebird/

Debian/Ubuntu:

    /usr/include/

FreeBSD:

    /usr/local/include/

OS X:

    /Library/Frameworks/Firebird.framework/Versions/A/Headers/


Linking libfq
-------------

`libfq` depends on the Firebird client library `libfbclient`; be sure
to link `libfq` before `libfbclient` (`-lfq -lfbclient`), and that libfbclient
is available to the linker (on OS X it is well hidden [1]).

[1] /Library/Frameworks/Firebird.framework/Versions/A/Libraries/
