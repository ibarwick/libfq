# libfq - configure.ac


AC_INIT([libfq], [0.6.0], [barwick@gmail.com])

AM_MAINTAINER_MODE([disable])

AC_CONFIG_AUX_DIR([config])
AM_INIT_AUTOMAKE([-Wall foreign])

ibase=/usr/include
fbclient=/usr/lib

AC_SUBST(ibase)
AC_SUBST(fbclient)

AC_PROG_CC
AM_PROG_AR
AC_PROG_LIBTOOL

AC_CONFIG_HEADER(include/config.h)


AC_CONFIG_FILES([Makefile src/Makefile])

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

AC_ARG_WITH(
    fbclient,
    AS_HELP_STRING(
        [--with-fbclient=DIR],
        [directory containing fbclient library]
    ),
    [fbclient="$withval"]
)





AC_OUTPUT
