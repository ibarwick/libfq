## Makefile.am - process with automake to produce Makefile.in

AUTOMAKE_OPTIONS = subdir-objects

SUBDIRS = lib

ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS        = -I $(top_builddir)/include

include_HEADERS = include/c.h  include/fqexpbuffer.h  include/libfq.h  include/libfq-int.h

lib_LTLIBRARIES = libfq.la
libfq_la_SOURCES = lib/libfq.c lib/fqexpbuffer.c
libfq_la_LDFLAGS=-version-info 0:1:0


