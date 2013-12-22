# libfq - configure.ac

AC_PREREQ([2.59])
AC_INIT([libfq], [0.0.1], [barwick@sql-info.de])
AM_INIT_AUTOMAKE([-Wall foreign])

AC_CONFIG_SRCDIR(lib)

AC_PROG_CC
AM_PROG_AR
AC_PROG_LIBTOOL

AC_CONFIG_MACRO_DIR([m4])

AC_CONFIG_FILES([Makefile lib/Makefile])

AC_CHECK_HEADERS([fcntl.h limits.h string.h stdlib.h math.h sys/time.h unistd.h])
AC_CHECK_FUNCS([memmove memset strchr strdup strstr strtol atoi atof])

#LT_INIT

AC_OUTPUT
