#ifndef LIBFQ_INT_H
#define LIBFQ_INT_H

typedef PARAMVARY VARY2;
typedef unsigned int fb_wchar;

#define ERROR_BUFFER_LEN 512

#define FB_FLOAT_LEN 16
#define FB_DOUBLE_LEN 25
#define FB_INT128_LEN 46
#define FB_DATE_LEN 10
#define FB_TIME_LEN 13
#define FB_TIMESTAMP_LEN 24

/* Initial number of XSQLVARs to allocate for an XSQLDA.
 * There is a small memory overhead associated with each XSQLVAR record,
 * but it's probably better to pre-allocated a reasonable number than
 * dynamically reallocate.
 */
#define FB_XSQLDA_INITLEN 15

/*
 * INT64 sscanf formats for various platforms
 */

#if defined(_MSC_VER)        /* Microsoft C compiler/library */
#  define S_INT64_FULL        "%%I64d.%%%dI64d%%1I64d"
#  define S_INT64_NOSCALE     "%%I64d.%%1I64d"
#  define S_INT64_DEC_FULL    ".%%%dI64d%%1I64d"
#  define S_INT64_DEC_NOSCALE ".%%1I64d"
#elif defined (__FreeBSD__)  /* FreeBSD */
#  define S_INT64_FULL        "%%qd.%%%dqd%%1qd"
#  define S_INT64_NOSCALE     "%%qd.%%1qd"
#  define S_INT64_DEC_FULL    ".%%%dqd%%1qd"
#  define S_INT64_DEC_NOSCALE ".%%1qd"
#else                        /* others: linux, various unices */
#  define S_INT64_FULL        "%%lld.%%%dlld%%1lld"
#  define S_INT64_NOSCALE     "%%lld.%%1lld"
#  define S_INT64_DEC_FULL    ".%%%dlld%%1lld"
#  define S_INT64_DEC_NOSCALE ".%%1lld"
#endif

extern int pg_utf_dsplen(const unsigned char *s);

extern int pg_utf_mblen(const unsigned char *s);

#endif   /* LIBFQ_INT_H */
