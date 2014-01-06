#ifndef LIBFQ_H
#define LIBFQ_H


#include <ibase.h>

#include "fqexpbuffer.h"

#ifndef C_H
#include "c.h"
#endif


/* Using the same loglevel constants as PostgreSQL
 * (though we don't need the server-only ones)
 *
 */
#ifndef DEBUG5
#define DEBUG5          10   /* Debugging messages, in order of decreasing detail. */
#define DEBUG4          11
#define DEBUG3          12
#define DEBUG2          13
#define DEBUG1          14
#define INFO            17   /* Messages specifically requested by user; always sent to client
                              * client regardless of client_min_messages */
#define NOTICE          18   /* Helpful messages to users about query operation;  */
#define WARNING         19   /* Unexpected but non-fatal messages. */
#define ERROR           20   /* User error - abort transaction; return to known state */
#define FATAL           21   /* Fatal error - abort process */
#define PANIC           22   /* Should never happen... */
#endif


/* From ibase.h */
#define SQL_TEXT                           452
#define SQL_VARYING                        448
#define SQL_SHORT                          500
#define SQL_LONG                           496
#define SQL_FLOAT                          482
#define SQL_DOUBLE                         480
#define SQL_D_FLOAT                        530
#define SQL_TIMESTAMP                      510
#define SQL_BLOB                           520
#define SQL_ARRAY                          540
#define SQL_QUAD                           550
#define SQL_TYPE_TIME                      560
#define SQL_TYPE_DATE                      570
#define SQL_INT64                          580
#define SQL_NULL                         32766

/* pseudo-type for convenience */
#define SQL_INVALID_TYPE                    -1
/* libfq customisation to indicate a column represents an RDB$DB_KEY value */
#define SQL_DB_KEY                       16384
#define FB_DB_KEY_LEN                       16

typedef enum
{
    CONNECTION_OK = 0,
    CONNECTION_BAD
} FQconnStatusType;


typedef enum {
    FBRES_NO_ACTION = 0,
    FBRES_EMPTY_QUERY,
    FBRES_COMMAND_OK,
    FBRES_TUPLES_OK,
    FBRES_TRANSACTION_START,
    FBRES_TRANSACTION_COMMIT,
    FBRES_TRANSACTION_ROLLBACK,
    FBRES_BAD_RESPONSE,
    FBRES_NONFATAL_ERROR,
    FBRES_FATAL_ERROR
} FQexecStatusType;

typedef enum {
    FB_DIAG_OTHER = 0,
    FB_DIAG_MESSAGE_PRIMARY,
    FB_DIAG_MESSAGE_DETAIL,
    FB_DIAG_MESSAGE_LINE,
    FB_DIAG_MESSAGE_COLUMN,
    FB_DIAG_DEBUG  /* debugging info, not usually displayed */
} FQdiagType;


typedef enum {
    TRANS_OK,
    TRANS_ERROR
} FQtransactionStatusType;


typedef struct FQconn {
    isc_db_handle  db;
    isc_tr_handle  trans;
    isc_tr_handle  trans_internal; /* transaction handle for atomic internal operations */
    bool           autocommit;
    bool           in_user_transaction; /* set when explicit SET TRANSACTION executed */
    char          *dpb_buffer;
    short          dpb_length;
    ISC_STATUS    *status;
    char          *engine_version;        /* Firebird version as reported by rdb$get_context() */
    int            engine_version_number; /* integer representation of Firebird version */
    short          client_min_messages;
} FQconn;



typedef struct FQresTupleAtt
{
    char *value;
    int len;
    bool has_null;
    char *value_sanitized;
} FQresTupleAtt;


typedef struct FQresTuple
{
    FQresTupleAtt     **values;
    int                 position;
    struct FQresTuple  *next;
} FQresTuple;


typedef struct FQresTupleAttDesc
{
    char  *desc;        /* column name */
    short  desc_len;    /* length of column name */
    char  *alias;       /* column alias, if provided */
    short  alias_len;
    int    att_max_len; /* max length of value in column */
    short  type;        /* datatype */
    bool   has_null;    /* indicates if resultset contains at least one NULL */
} FQresTupleAttDesc;

/* Typedef for message-field list entries */
typedef struct fbMessageField
{
    struct fbMessageField *prev;    /* list link */
    struct fbMessageField *next;    /* list link */
    FQdiagType             code;    /* field code */
    char                  *value;   /* field value */
} FBMessageField;


/* Initialised with _FQinitResult() */
typedef struct FQresult
{
    XSQLDA *sqlda_out;              /* Pointer to a Firebird XSQLDA structure used to hold output tuple information */
    XSQLDA *sqlda_in;               /* Pointer to a Firebird XSQLDA structure used to hold data for prepared statements */
                                    /* NOTE: the XSQLDA pointers are only used during query execution and will be
                                     * freed once execution has completed; see _FQexecClearResult() */
    char *sqlda_out_buffer;         /* Temporary buffer to hold output tuple data */
    isc_stmt_handle stmt_handle;
    FQexecStatusType resultStatus;
    int ntups;                      /* The number of rows (tuples) returned by a query.
                                     * Will be -1 until a valid query is executed. */
    int ncols;                      /* The number of columns in the result tuples.
                                     * Will be -1 until a valid query is executed. */

    struct FQresTupleAttDesc **header;
    struct FQresTuple **tuples;     /* Array of pointers to returned tuples */

    struct FQresTuple *tuple_first; /* Pointer to first returned tuple */
    struct FQresTuple *tuple_last;  /* Pointer to last returned tuple */

    /*
     * Error information (all NULL if not an error result).  errMsg is the
     * "overall" error message returned by FQresultErrorMessage.  If we have
     * per-field info then it is stored in a linked list.
     */
    char       *errMsg;         /* error message, or NULL if no error */
    FBMessageField *errFields;  /* message broken into fields */
    long       fbSQLCODE;       /* Firebird SQL code */
} FQresult;

extern char *const fbresStatus[];

extern FQconn *
FQconnect(char *db_path, char *uname, char *upass);

extern void
FQfinish(FQconn *conn);

extern FQconnStatusType
FQstatus(const FQconn *conn);

extern char *
FQerrorMessage(const FQconn *conn);

extern char *
FQresultErrorMessage(const FQresult *res);

extern char *
FQresultErrorField(const FQresult *res, FQdiagType fieldcode);

extern char *
FQresultErrorFieldsAsString(const FQresult *res, char *prefix);

extern void
FQresultDumpErrorFields(FQconn *conn, const FQresult *res);

extern int
FQserverVersion(FQconn *conn);

extern char *
FQserverVersionString(FQconn *conn);

extern FQresult *
FQexec(FQconn *conn, const char *stmt);

extern FQresult *
FQexecParams(FQconn *conn,
             const char *stmt,
             int nParams,
             const int *paramTypes,
             const char * const *paramValues,
             const int *paramLengths,
             const int *paramFormats,
             int resultFormat
    );


extern FQresult *
FQexecTransaction(FQconn *conn, const char *stmt);

extern char *
FQexecSingleItemQuery(FQconn *conn, const char *stmt);

extern int
FQntuples(const FQresult *res);

extern int
FQnfields(const FQresult *res);


extern bool
FQfhasNull(const FQresult *res, int column_number);

extern int
FQfmaxwidth(const FQresult *res, int column_number);

extern char *
FQfname(const FQresult *res, int column_number);

extern short
FQfformat(const FQresult *res, int column_number);

extern short
FQftype(const FQresult *res, int column_number);


extern int
FQgetlength(const FQresult *res,
            int row_number,
            int column_number);

extern char *
FQgetvalue(const FQresult *res,
           int row_number,
           int column_number);

extern int
FQgetisnull(const FQresult *res,
            int row_number,
            int column_number);

extern char *
FQformatDbKey(const FQresult *res,
              int row_number,
              int column_number);

extern FQexecStatusType
FQresultStatus(const FQresult *res);

extern char*
FQresStatus(FQexecStatusType status);

extern bool
FQisActiveTransaction(FQconn *conn);

extern void
FQsetAutocommit(FQconn *conn, bool autocommit);

extern FQtransactionStatusType
FQcommitTransaction(FQconn *conn);

extern FQtransactionStatusType
FQrollbackTransaction(FQconn *conn);

extern FQtransactionStatusType
FQstartTransaction(FQconn *conn);

extern char *
FQexplainStatement(FQconn *conn, const char *stmt);

extern void
FQclear(FQresult *res);


extern void
FQlog(FQconn *conn, short loglevel, const char *msg, ...);

/* handling for character/encoding */

extern int
FQmblen(const char *s, int encoding);

#endif   /* LIBFQ_H */
