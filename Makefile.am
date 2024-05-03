## Makefile.am - process with automake to produce Makefile.in

AUTOMAKE_OPTIONS = subdir-objects

SUBDIRS = src

#ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS        = -I$(top_builddir)/include -I$(ibase)
AM_LDFLAGS         = -lfbclient -L$(fbclient)

include_HEADERS = include/libfq-expbuffer.h include/libfq.h include/libfq-int.h

lib_LTLIBRARIES = libfq.la
libfq_la_SOURCES = src/libfq.c src/fqexpbuffer.c src/fqmultibyte.c
libfq_la_LDFLAGS = -release 0.6.0 -lfbclient -L$(fbclient)


