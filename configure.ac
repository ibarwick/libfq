# libfq - configure.ac

AC_PREREQ([2.59])
AC_INIT([libfq], [0.1.0], [barwick@sql-info.de])

AC_CONFIG_AUX_DIR([config])
AM_INIT_AUTOMAKE([-Wall foreign])



ibase=/usr/include
AC_SUBST(ibase)

AC_PROG_CC
AM_PROG_AR
AC_PROG_LIBTOOL

AC_CONFIG_MACRO_DIR([m4])

AC_CONFIG_FILES([Makefile lib/Makefile])

AC_CHECK_HEADERS([fcntl.h limits.h string.h stdlib.h math.h sys/time.h unistd.h])
AC_CHECK_FUNCS([memmove memset strchr strdup strstr strtol atoi atof])


AC_ARG_WITH(
    ibase,
    AS_HELP_STRING(
        [--with-ibase=DIR],
        [directory containing ibase.h]
    ),
    [ibase="$withval"]
)



AC_OUTPUT
