/*----------------------------------------------------------------------
 *
 * libfq - C API wrapper for Firebird
 *
 * Copyright (c) 2013-2023 Ian Barwick
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Ian Barwick <barwick@gmail.com>
 *
 * NOTES
 * -----
 *
 * Version-dependent datatype support: the presence of datatypes added in
 * Firebird 3.0 or later is detected based on the existence of the respective
 * constant (defined in "sqlda_pub.h", which is included from "ibase.h").
 * This enables libfq to be built against older Firebird versions, if
 * necessary.
 *
 * Currently following version-dependent datatypes are supported
 * - BOOLEAN (Firebird 3.0; constant: SQL_BOOLEAN)
 * - INT128  (Firebird 4.0; constant: SQL_INT128)
 * - TIME[STAMP] WITH TIME ZONE (Firebird 4.0; constant: SQL_TIMESTAMP_TZ)
 *----------------------------------------------------------------------
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <math.h>
#include <unistd.h>
#include <ctype.h>

#include "ibase.h"

#include "libfq-int.h"
#include "libfq.h"
#include "libfq-version.h"

/* Available from Firebird 4.0 */
#ifdef SQL_TIMESTAMP_TZ

#define HAVE_TIMEZONE
#include "libfq-timezones.h"

#endif


typedef struct log_level_entry {
	const char *log_level;
	int log_level_id;
} log_level_entry;

struct log_level_entry log_levels[] = {
	{ "DEBUG5",  DEBUG5 },
	{ "DEBUG4",  DEBUG4 },
	{ "DEBUG3",  DEBUG3 },
	{ "DEBUG2",  DEBUG2 },
	{ "DEBUG1",  DEBUG1 },
	{ "INFO",    INFO },
	{ "NOTICE",  NOTICE },
	{ "WARNING", WARNING },
	{ "ERROR",   ERROR },
	{ "FATAL",   FATAL },
	{ "PANIC",   PANIC},
	{ NULL, 0 }
};


/* Internal utility functions */

static void
_FQserverVersionInit(FBconn *conn);

static FQtransactionStatusType
_FQcommitTransaction(FBconn *conn, isc_tr_handle *trans);
static FQtransactionStatusType
_FQrollbackTransaction(FBconn *conn, isc_tr_handle *trans);
static FQtransactionStatusType
_FQstartTransaction(FBconn *conn, isc_tr_handle *trans);

static FQresTupleAtt *_FQformatDatum (FBconn *conn, FQresTupleAttDesc *att_desc, XSQLVAR *var);
static FBresult *_FQinitResult(bool init_sqlda_in);
static void _FQinitResultSqlDa(FBresult *result, bool init_sqlda_in);
static void _FQexecClearResult(FBresult *result);
static void _FQexecClearResultParams(FBconn *conn, FBresult *result, bool free_result_stmt_handle);
static void _FQexecClearSQLDA(FBresult *result, XSQLDA *sqlda);
static void _FQexecFillTuplesArray(FBresult *result);
static void _FQexecInitOutputSQLDA(FBconn *conn, FBresult *result);
static ISC_LONG _FQexecParseStatementType(char *info_buffer);

static FBresult *_FQexec(FBconn *conn, isc_tr_handle *trans, const char *stmt);
static FBresult *_FQexecParams(FBconn *conn,
							   isc_tr_handle *trans,
							   //const char *stmt,
							   FBresult	 *result,
							   bool free_result_stmt_handle,
							   int nParams,
							   const char * const *paramValues,
							   const int *paramLengths,
							   const int *paramFormats,
							   int resultFormat);

static void _FQstoreResult(FBresult *result, FBconn *conn, int num_rows);
static char *_FQlogLevel(short errlevel);
static void _FQsetResultError(FBconn *conn, FBresult *res);
static void _FQsetResultNonFatalError(const FBconn *conn, FBresult *res, short errlevel, char *msg);
static void _FQsaveMessageField(FBresult **res, FQdiagType code, const char *value, ...);
static char *_FQdeparseDbKey(const char *db_key);
static char *_FQparseDbKey(const char *db_key);

static void _FQinitClientEncoding(FBconn *conn);
static const char *_FQclientEncoding(FBconn *conn);

static int _FQdspstrlen_line(FQresTupleAtt *att, short encoding_id);

static int check_tuple_field_number(const FBresult *res,
									int row_number, int column_number);

static int _FQgetLogLevelFromName(const char *log_level);
static const char*  _FQgetLogLevelName(int log_level);

#if defined SQL_INT128
static int format_int128(__int128 val, char *dst);
static __int128 convert_int128(const char *s);
#endif

#ifdef HAVE_TIMEZONE
static char *_FQlookupTimeZone(int time_zone_id);
static char *_FQformatTimeZone(int time_zone_id, int tz_ext_offset, bool time_zone_names);
#endif

static char *_FQformatOctet(char *data, int len);


/* keep this in same order as FQexecStatusType in libfq.h */
char *const fbresStatus[] = {
	"FBRES_NO_ACTION",
	"FBRES_EMPTY_QUERY",
	"FBRES_COMMAND_OK",
	"FBRES_TUPLES_OK",
	"FBRES_TRANSACTION_START",
	"FBRES_TRANSACTION_COMMIT",
	"FBRES_TRANSACTION_ROLLBACK",
	"FBRES_BAD_RESPONSE",
	"FBRES_NONFATAL_ERROR",
	"FBRES_FATAL_ERROR"
};


/*
 * =====================================
 * Database Connection Control Functions
 * =====================================
 */

/**
 * FQconnect()
 *
 * Simple function to create a connection to a Firebird database
 * providing only the database path, username and password.
 *
 * FQconnectdbParams() is an alternative function providing more
 * connection options.
 */
FBconn *
FQconnect(const char *db_path, const char *uname, const char *upass)
{
	const char *kw[4];
	const char *val[4];

	kw[0] = "db_path";
	val[0] = db_path;

	kw[1] = "user";
	val[1] = uname;

	kw[2] = "password";
	val[2] = upass;

	kw[3] = val[3] = NULL;

	return FQconnectdbParams(kw, val);
}


/**
 * FQconnectdbParams()
 *
 * Establish a new server connection using parameters provided as
 * a pair of NULL-terminated arrays.
 *
 * Parameters currently recognized are:
 *
 *  db_path
 *  user
 *  password
 *  client_encoding
 *  time_zone_names
 *
 * This list may change in the future.
 *
 * The number of supported parameters is defined in FBCONN_MAX_PARAMS.
 */
FBconn *
FQconnectdbParams(const char * const *keywords,
                  const char * const *values)
{
	FBconn *conn;

	size_t db_path_len;
	char *dpb;

	const char *db_path = NULL;

	/* XXX: make options into a nice struct */
	const char *uname = NULL;
	const char *upass = NULL;
	const char *client_encoding = NULL;
	bool  time_zone_names = false;
	int client_min_messages = DEBUG1;

	int i = 0;

	while (keywords[i])
	{
		if (strcmp(keywords[i], "db_path") == 0)
			db_path = values[i];
		else if (strcmp(keywords[i], "user") == 0)
			uname = values[i];
		else if (strcmp(keywords[i], "password") == 0)
			upass = values[i];
		else if (strcmp(keywords[i], "client_encoding") == 0)
			client_encoding = values[i];
		else if (strcmp(keywords[i], "client_min_messages") == 0)
			client_min_messages = _FQgetLogLevelFromName(values[i]);
		else if (strcmp(keywords[i], "time_zone_names") == 0)
			/* XXX better boolean parsing? */
			time_zone_names = strncmp(values[i], "true", 5) == 0 ? true : false;

		i++;
	}

	/* db_path is required */
	if (db_path == NULL)
		return NULL;

	/* initialise libfq's connection struct */
	conn = (FBconn *)malloc(sizeof(FBconn));

	conn->db = 0L;
	conn->trans = 0L;
	conn->trans_internal = 0L;
	conn->autocommit = true;
	conn->in_user_transaction = false;
	conn->status = (ISC_STATUS *) malloc(sizeof(ISC_STATUS) * ISC_STATUS_LENGTH);
	conn->engine_version = NULL;
	conn->client_min_messages = client_min_messages;
	conn->client_encoding = NULL;
	conn->client_encoding_id = FBENC_UNKNOWN;	/* indicate the server-parsed value has not yet been retrieved */
	conn->get_dsp_len = false;
	conn->uname = NULL;
	conn->upass = NULL;
	conn->time_zone_names = time_zone_names;
	conn->errMsg = NULL;

	/* Initialise the Firebird parameter buffer */
	conn->dpb_buffer = (char *) malloc((size_t)256);

	dpb = (char *)conn->dpb_buffer;

	*dpb++ = isc_dpb_version1;

	conn->dpb_length = dpb - (char*)conn->dpb_buffer;
	dpb = (char *)conn->dpb_buffer;

	/* store database path */
	db_path_len = strlen(db_path);
	conn->db_path = malloc(db_path_len + 1);
	strncpy(conn->db_path, db_path, db_path_len);
	conn->db_path[db_path_len] = '\0';

	/* set and store other parameters */
	if (uname != NULL)
	{
		int uname_len = strlen(uname);

		isc_modify_dpb(&dpb, &conn->dpb_length, isc_dpb_user_name, uname, uname_len);

		conn->uname = malloc(uname_len + 1);
		strncpy(conn->uname, uname, uname_len);
		conn->uname[uname_len] = '\0';
	}

	if (upass != NULL)
	{
		int upass_len = strlen(upass);

		isc_modify_dpb(&dpb, &conn->dpb_length, isc_dpb_password, upass, upass_len);

		conn->upass = malloc(upass_len + 1);
		strncpy(conn->upass, upass, upass_len);
		conn->upass[upass_len] = '\0';
	}

	/*
	 * Set client encoding.
	 *
	 * Note that the encoding string and corresponding ID will be cached in the
	 * connection object the first time FQclientEncodingId() is called.
	 */
	if (client_encoding == NULL)
	{
		/*
		 * Default to UTF8; apps should provide a way of overriding this
		 * if necessary. It would be nice to parse the LC_TYPE environment
		 * variable etc. along the lines of PostgreSQL's internal function
		 * pg_get_encoding_from_local().
		 */
		client_encoding = "UTF8";
	}

	isc_modify_dpb(&dpb, &conn->dpb_length, isc_dpb_lc_ctype, client_encoding, strlen(client_encoding));

	/* actually attach to the database */
	isc_attach_database(
		conn->status,
		0,
		db_path,
		&conn->db,
		conn->dpb_length,
		dpb
	);

	if (conn->status[0] == 1 && conn->status[1])
	{
		long *pvector;
		char msg[ERROR_BUFFER_LEN];
		FQExpBufferData buf;
		int line = 0;
		int msg_len = 0;

		initFQExpBuffer(&buf);

		/* fb_interpret() will modify this pointer */
		pvector = conn->status;

		while (fb_interpret(msg, ERROR_BUFFER_LEN, (const ISC_STATUS**) &pvector))
		{
			if (line == 0)
			{
				appendFQExpBuffer(&buf, "%s\n", msg);
			}
			else
			{
				appendFQExpBuffer(&buf, " - %s\n", msg);
			}

			line++;
		}

		msg_len = strlen(buf.data);

		if (conn->errMsg != NULL)
		{
			free(conn->errMsg);
		}

		conn->errMsg = (char *)malloc(msg_len + 1);
		memset(conn->errMsg, '\0', msg_len + 1);
		strncpy(conn->errMsg, buf.data, msg_len);

		termFQExpBuffer(&buf);
	}
	else
	{
		_FQinitClientEncoding(conn);

#ifdef HAVE_TIMEZONE

		/*
		 * With Firebird 4 and later, set time zones to "extended" format so
		 * we always have the numeric timezone offset available.
		 *
		 * Note: in theory something like this (before attaching to the database)
		 * should work:
		 *
		 *   char *bindtest = "TIME ZONE TO EXTENDED";
		 * 	 isc_modify_dpb(&dpb, &conn->dpb_length, isc_dpb_set_bind, bindtest, strlen(bindtest));
		 *
		 * but doesn't.
		 */

		if (FQserverVersion(conn) >= 40000)
			FQexec(conn, "SET BIND OF TIME ZONE TO EXTENDED");
#endif
	}

	return conn;
}


/**
 * FQreconnect()
 *
 * Actually create a new connection with the parameters of the connection
 * provided; it's up to the caller to FQclear() the old connection.
 *
 * NOTE: ideally we'd just modify the provided connection in-situ, but
 * haven't worked out the correct incantations for doing this yet...
 */

FBconn *
FQreconnect(FBconn *conn)
{
	const char *kw[5];
	const char *val[5];
	int i = 0;
	FBconn *new_conn;

	if (conn == NULL)
		return NULL;

	kw[i] = "db_path";
	val[i] = conn->db_path;
	i++;

	if (conn->uname != NULL)
	{
		kw[i] = "user";
		val[i] = conn->uname;
		i++;
	}

	if (conn->upass != NULL)
	{
		kw[i] = "password";
		val[i] = conn->upass;
		i++;
	}

	if (conn->client_encoding != NULL)
	{
		kw[i] = "client_encoding";
		val[i] = conn->client_encoding;
		i++;
	}

	kw[i] = NULL;
	val[i] = NULL;

	new_conn = FQconnectdbParams(kw, val);

	return new_conn;
}


/**
 * FQfinish()
 *
 * Detach from database if connected and free the connection
 * handle
 */
void
FQfinish(FBconn *conn)
{
	if (conn == NULL)
		return;

	if (conn->trans != 0L)
		FQrollbackTransaction(conn);

	if (conn->db != 0L)
		isc_detach_database(conn->status, &conn->db);

	if (conn->status != NULL)
		free(conn->status);

	if (conn->dpb_buffer != NULL)
		free(conn->dpb_buffer);

	if (conn->engine_version != NULL)
		free(conn->engine_version);

	if (conn->db_path != NULL)
		free(conn->db_path);

	if (conn->uname != NULL)
		free(conn->uname);

	if (conn->upass != NULL)
		free(conn->upass);

	if (conn->client_encoding != NULL)
		free(conn->client_encoding);

	if (conn->errMsg != NULL)
		free(conn->errMsg);

	free(conn);
}


/**
 * FQsetTimeZoneNames()
 *
 * Indicate whether to return time zone names, where available.
 */
int
FQsetTimeZoneNames(FBconn *conn, bool time_zone_names)
{
	if (conn == NULL)
		return FQ_SET_NO_DB;

	conn->time_zone_names = time_zone_names;
	return FQ_SET_SUCCESS;
}

/**
 * FQsetClientMinMessages()
 */
int
FQsetClientMinMessages(FBconn *conn, int log_level)
{

	if (conn == NULL)
		return FQ_SET_NO_DB;

	conn->client_min_messages = log_level;
	return FQ_SET_SUCCESS;

}


int
FQsetClientMinMessagesString(FBconn *conn, const char *log_level)
{
	int log_level_id;

	if (conn == NULL)
		return FQ_SET_NO_DB;

	log_level_id = _FQgetLogLevelFromName(log_level);

	if (log_level_id == 0)
		return FQ_SET_ERROR;

	conn->client_min_messages = log_level_id;

	return FQ_SET_SUCCESS;
}

/*
 * ====================================
 * Database Connection Status Functions
 * ====================================
 */

/**
 * FQstatus()
 *
 * Determines whether the provided connection object
 * has an active connection.
 */
FBconnStatusType
FQstatus(FBconn *conn)
{
	char db_items[] =
	{
		isc_info_page_size,
		isc_info_num_buffers,
		isc_info_end
	};

	char res_buffer[40];

	/* connection object not initialised, or database handle was never set */
	if (conn == NULL || conn->db == 0L)
		return CONNECTION_BAD;

	/* (mis)use isc_database_info() to see if the connection is still active */

	isc_database_info(
		conn->status,
		&conn->db,
		sizeof(db_items),
		db_items,
		sizeof(res_buffer),
		res_buffer);

	if (conn->status[0] == 1 && conn->status[1])
	{
		return CONNECTION_BAD;
	}

	return CONNECTION_OK;
}


/**
 * FQparameterStatus()
 *
 * Returns a current parameter setting from the provided connection object
 */

extern const char *
FQparameterStatus(FBconn *conn, const char *paramName)
{
	if (conn == NULL)
		return NULL;

	if (strcmp(paramName, "client_encoding") == 0)
		return _FQclientEncoding(conn);

	if (strcmp(paramName, "time_zone_names") == 0)
		return conn->time_zone_names == true ? "enabled" : "disabled";


	if (strcmp(paramName, "client_min_messages") == 0)
	{
		const char *log_level = _FQgetLogLevelName(conn->client_min_messages);

		return log_level ? log_level : "unknown log level";
	}

	return NULL;
}


const char *FQdb_path(const FBconn *conn)
{
	return conn->db_path;
}

const char *FQuname(const FBconn *conn)
{
	return conn->uname;
}

const char *FQupass(const FBconn *conn)
{
	return conn->upass;
}



/**
 * FQserverVersion()
 *
 * Return the reported server version number as an integer suitable for
 * comparision, e.g. 2.5.2 = 20502
 */
int
FQserverVersion(FBconn *conn)
{
	if (conn == NULL)
		return -1;

	_FQserverVersionInit(conn);

	return conn->engine_version_number;
}


/**
 * FQserverVersionString()
 *
 * Return the reported server version as a string (e.g. "2.5.2")
 */
char *
FQserverVersionString(FBconn *conn)
{
	if (conn == NULL)
		return NULL;

	_FQserverVersionInit(conn);

	return conn->engine_version;
}


/**
 * _FQserverVersionInit()
 *
 * Executes a query to extract the database server version,
 * and stores the results in the connection object.
 *
 * This is called by FQserverVersion() and FQserverVersionString()
 * so the version information is extracted on demand, not on every
 * connection initialisation.
 */
void
_FQserverVersionInit(FBconn *conn)
{
	const char *sql = "SELECT CAST(rdb$get_context('SYSTEM', 'ENGINE_VERSION') AS VARCHAR(10)) FROM rdb$database";

	if (conn->engine_version == NULL)
	{
		/* Extract the server version code and cache it in the connection handle */

		FBresult   *res;

		if (_FQstartTransaction(conn, &conn->trans_internal) == TRANS_ERROR)
			return;

		res = _FQexec(conn, &conn->trans_internal, sql);

		if (FQresultStatus(res) == FBRES_TUPLES_OK && !FQgetisnull(res, 0, 0))
		{
			int major, minor, revision;
			char buf[10] = "";
			int engine_version_len = sizeof(FQgetvalue(res, 0, 0));

			conn->engine_version = malloc(engine_version_len + 1);
			strncpy(conn->engine_version, FQgetvalue(res, 0, 0), engine_version_len);
			conn->engine_version[engine_version_len] = '\0';

			if (sscanf(conn->engine_version, "%i.%i.%i", &major, &minor, &revision) == 3)
			{
				sprintf(buf, "%d%02d%02d", major, minor, revision);
			}
			else
			{
				sprintf(buf, "0");
			}
			conn->engine_version_number = atoi(buf);
		}
		else
		{
			conn->engine_version = malloc(1);
			conn->engine_version[0] = '\0';
			conn->engine_version_number = -1;
		}
		FQclear(res);
		_FQcommitTransaction(conn, &conn->trans_internal);
	}
}

/**
 * FQclientEncodingId()
 *
 */
int
FQclientEncodingId(FBconn *conn)
{
	if (conn == NULL)
		return FBENC_UNKNOWN;

	if (conn->client_encoding_id == FBENC_UNKNOWN)
		_FQinitClientEncoding(conn);

	return conn->client_encoding_id;
}


/**
 * _FQclientEncoding()
 *
 */
static const char *
_FQclientEncoding(FBconn *conn)
{
	if (conn == NULL)
		return "n/a";

	if (conn->client_encoding_id == FBENC_UNKNOWN)
		_FQinitClientEncoding(conn);

	return conn->client_encoding;
}

/**
 * _FQinitClientEncoding()
 *
 */
static void
_FQinitClientEncoding(FBconn *conn)
{
	const char *query = \
"    SELECT TRIM(rdb$character_set_name) AS client_encoding, " \
"           mon$character_set_id AS client_encoding_id " \
"      FROM mon$attachments " \
"INNER JOIN rdb$character_sets " \
"        ON mon$character_set_id = rdb$character_set_id "\
"     WHERE mon$attachment_id = CURRENT_CONNECTION ";

	FBresult   *res;
	int client_encoding_len;

	if (_FQstartTransaction(conn, &conn->trans_internal) == TRANS_ERROR)
		return;

	res = _FQexec(conn, &conn->trans_internal, query);

	if (FQresultStatus(res) != FBRES_TUPLES_OK || !FQntuples(res) || FQgetisnull(res, 0, 0))
	{
		FQclear(res);

		_FQrollbackTransaction(conn, &conn->trans_internal);

		return;
	}

	client_encoding_len = strlen(FQgetvalue(res, 0, 0));

	if (conn->client_encoding != NULL)
		free(conn->client_encoding);

	conn->client_encoding =	malloc(client_encoding_len + 1);
	memset(conn->client_encoding, '\0', client_encoding_len + 1);
	memcpy(conn->client_encoding, FQgetvalue(res, 0, 0), client_encoding_len);

	/*
	 * If the query returns a row, this value will never be NULL.
	 */
	conn->client_encoding_id = (short)atoi(FQgetvalue(res, 0, 1));

	return;
}



/**
 * FQlibVersion()
 */
int
FQlibVersion(void)
{
	return LIBFQ_VERSION_NUMBER;
}


/**
 * FQlibVersionString()
 */

const char *
FQlibVersionString(void)
{
	return LIBFQ_VERSION_STRING;
}



/**
 * FQsetGetdsplen()
 *
 * Determine whether the display width for each datum is calculated.
 * This is convenient for applications (e.g. fbsql) which format tabular
 * output, but adds some overhead so is off by default.
 */
void
FQsetGetdsplen(FBconn *conn, bool get_dsp_len)
{
	conn->get_dsp_len = get_dsp_len;
}



/**
 * _FQinitResult()
 *
 * Initialise an FBresult object with sensible defaults and
 * preallocate in/out SQLDAs.
 */
static FBresult *
_FQinitResult(bool init_sqlda_in)
{
	FBresult *result;

	result = malloc(sizeof(FBresult));

	_FQinitResultSqlDa(result, init_sqlda_in);

	result->stmt_handle = 0L;
	result->statement_type = 0L;
	result->ntups = -1;
	result->ncols = -1;
	result->resultStatus = FBRES_NO_ACTION;
	result->errMsg = NULL;
	result->errFields = NULL;
	result->fbSQLCODE = -1L;
	result->errLine = -1;
	result->errCol = -1;

	return result;
}

static void
_FQinitResultSqlDa(FBresult *result, bool init_sqlda_in)
{
	if (init_sqlda_in == true)
	{
		result->sqlda_in = (XSQLDA *) malloc(XSQLDA_LENGTH(FB_XSQLDA_INITLEN));
		memset(result->sqlda_in, '\0', XSQLDA_LENGTH(FB_XSQLDA_INITLEN));
		result->sqlda_in->sqln = FB_XSQLDA_INITLEN;
		result->sqlda_in->version = SQLDA_VERSION1;
	}
	else
	{
		result->sqlda_in = NULL;
	}

	result->sqlda_out = (XSQLDA *) malloc(XSQLDA_LENGTH(FB_XSQLDA_INITLEN));
	memset(result->sqlda_out, '\0', XSQLDA_LENGTH(FB_XSQLDA_INITLEN));
	result->sqlda_out->sqln = FB_XSQLDA_INITLEN;
	result->sqlda_out->version = SQLDA_VERSION1;
}

/**
 * _FQexecClearResult()
 *
 * Free a result object's temporary memory allocations assigned
 * during query execution
 */
static void
_FQexecClearResult(FBresult *result)
{
	if (result->sqlda_in != NULL)
	{
		_FQexecClearSQLDA(result, result->sqlda_in);
		free(result->sqlda_in);
		result->sqlda_in = NULL;
	}

	if (result->sqlda_out != NULL)
	{
		_FQexecClearSQLDA(result, result->sqlda_out);

		free(result->sqlda_out);
		result->sqlda_out = NULL;
	}
}



static void
_FQexecClearResultParams(FBconn *conn, FBresult *result, bool free_result_stmt_handle)
{
	_FQexecClearResult(result);

	if (free_result_stmt_handle)
	{
		isc_dsql_free_statement(conn->status, &result->stmt_handle, DSQL_drop);
	}
	else
	{
		_FQinitResultSqlDa(result, true);
	}
}


/**
 * _FQexecClearSQLDA()
 *
 *
 */
static
void _FQexecClearSQLDA(FBresult *result, XSQLDA *sqlda)
{
	XSQLVAR *var;
	short	 i;

	for (i = 0, var = result->sqlda_out->sqlvar; i < result->ncols; var++, i++)
	{
		if (var->sqldata != NULL)
		{
			free(var->sqldata);
			var->sqldata = NULL;
		}

		if (var->sqltype & 1 && var->sqlind != NULL)
		{
			/* deallocate NULL status indicator if necessary */
			free(var->sqlind);
			var->sqlind = NULL;
		}
	}
}


/**
 * _FQexecInitOutputSQLDA()
 *
 * Initialise an output SQLDA to hold a retrieved row
 *
 * This initialises a storage location for each column and additionally
 * a flag to indicate NULL status if required.
 *
 * It might be slightly more efficient to calculate the total size of
 * required storage and allocate a single buffer, pointing each SQLVAR
 * and NULL status indicator to a location in that buffer, but that is
 * somewhat tricky to get right.
 */
static void
_FQexecInitOutputSQLDA(FBconn *conn, FBresult *result)
{
	XSQLVAR *var;
	short	 sqltype, i;
	char	 error_message[1024];

	for (i = 0, var = result->sqlda_out->sqlvar; i < result->ncols; var++, i++)
	{
		sqltype = (var->sqltype & ~1); /* drop flag bit for now */
		switch(sqltype)
		{
			case SQL_VARYING:
				var->sqldata = (char *)malloc(sizeof(char)*var->sqllen + 2);
				break;
			case SQL_TEXT:
				var->sqldata = (char *)malloc(sizeof(char)*var->sqllen);
				break;

			case SQL_SHORT:
				var->sqldata = (char *)malloc(sizeof(ISC_SHORT));
				break;
			case SQL_LONG:
				var->sqldata = (char *)malloc(sizeof(ISC_LONG));
				break;
			case SQL_INT64:
				var->sqldata = (char *)malloc(sizeof(ISC_INT64));
				break;

			case SQL_FLOAT:
				var->sqldata = (char *)malloc(sizeof(float));
				break;
			case SQL_DOUBLE:
				var->sqldata = (char *)malloc(sizeof(double));
				break;

			case SQL_TYPE_TIME:
				var->sqldata = (char *)malloc(sizeof(ISC_TIME));
				break;
#ifdef HAVE_TIMEZONE
			case SQL_TIME_TZ:
				var->sqldata = (char *)malloc(sizeof(ISC_TIME_TZ));
				break;
			case SQL_TIME_TZ_EX:
				var->sqldata = (char *)malloc(sizeof(ISC_TIME_TZ_EX));
				break;
#endif
			case SQL_TIMESTAMP:
				var->sqldata = (char *)malloc(sizeof(ISC_TIMESTAMP));
				break;
#ifdef HAVE_TIMEZONE
			case SQL_TIMESTAMP_TZ:
				var->sqldata = (char *)malloc(sizeof(ISC_TIMESTAMP_TZ));
				break;
			case SQL_TIMESTAMP_TZ_EX:
				var->sqldata = (char *)malloc(sizeof(ISC_TIMESTAMP_TZ_EX));
				break;
#endif
			case SQL_TYPE_DATE:
				var->sqldata = (char *)malloc(sizeof(ISC_DATE));
				break;

			case SQL_BLOB:
				var->sqldata = (char *)malloc(sizeof(ISC_QUAD));
				break;

#if defined SQL_BOOLEAN
			/* Firebird 3.0 and later */
			case SQL_BOOLEAN:
				var->sqldata = (char *)malloc(sizeof(FB_BOOLEAN));
				break;
#endif

#if defined SQL_INT128
			/* Firebird 4.0 and later */
			case SQL_INT128:
				var->sqldata = (char *)malloc(sizeof(__int128));
				break;
#endif
			default:
				sprintf(error_message, "Unhandled sqlda_out type: %i", sqltype);

				_FQsetResultError(conn, result);
				_FQsaveMessageField(&result, FB_DIAG_DEBUG, error_message);

				result->resultStatus = FBRES_FATAL_ERROR;

				_FQexecClearResult(result);

				return;

		}
		if (var->sqltype & 1)
		{
			/* allocate variable to hold NULL status */
			var->sqlind = (short *)malloc(sizeof(short));
		}
	}

}


/**
 * _FQexecParseStatementType()
 *
 * info_buffer contains a isc_info_sql_stmt_type in the first byte,
 * two bytes of length, and a statement_type token.
 */
static ISC_LONG
_FQexecParseStatementType(char *info_buffer)
{
	short l = (short) isc_vax_integer((char *) info_buffer + 1, 2);
	return isc_vax_integer((char *) info_buffer + 3, l);
}


/**
 * _FQexecFillTuplesArray()
 *
 * Create an array of tuple pointers to provide fast offset-based
 * access to individual tuples.
 */
static void
_FQexecFillTuplesArray(FBresult *result)
{
	FQresTuple	  *tuple_ptr;
	int i;

	result->tuples = malloc(sizeof(FQresTuple *) * result->ntups);
	tuple_ptr = result->tuple_first;
	for (i = 0; i < result->ntups; i++)
	{
		result->tuples[i] = tuple_ptr;
		tuple_ptr = tuple_ptr->next;
	}
}


/**
 * FQexec()
 *
 * Execute the query specified in 'stmt'. Note that only one query
 * can be provided.
 *
 * Returns NULL when no server connection available.
 *
 * This function is a wrapper around _FQexec(), and calls it with the
 * connection's default transaction handle.
 *
 * To execute parameterized queries, use FQexecParams().
 */
FBresult *
FQexec(FBconn *conn, const char *stmt)
{
	if (!conn)
	{
		return NULL;
	}

	return(_FQexec(conn, &conn->trans, stmt));
}


/**
 * _FQexec()
 *
 * Execute the query specified in 'stmt' using the transaction handle
 * pointed to by 'trans'
 */
static FBresult *
_FQexec(FBconn *conn, isc_tr_handle *trans, const char *stmt)
{
	FBresult	  *result;

	static char	  stmt_info[] = { isc_info_sql_stmt_type };
	char		  info_buffer[20];
	int			  statement_type;

	int			  num_rows = 0;
	ISC_STATUS    retcode;

	bool		  temp_trans = false;

	result = _FQinitResult(false);

	/* Allocate a statement. */
	if (isc_dsql_allocate_statement(conn->status, &conn->db, &result->stmt_handle))
	{
		result->resultStatus = FBRES_FATAL_ERROR;
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - isc_dsql_allocate_statement");
		_FQsetResultError(conn, result);

		_FQexecClearResult(result);
		return result;
	}

	/* An active transaction is required to prepare the statement -
	 * if no transaction handle was provided by the caller,
	 * start a temporary transaction
	 */
	if (*trans == 0L)
	{
		_FQstartTransaction(conn, trans);
		temp_trans = true;
	}

	/* Prepare the statement. */
	if (isc_dsql_prepare(conn->status, trans, &result->stmt_handle, 0, stmt, SQL_DIALECT_V6, result->sqlda_out))
	{
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - isc_dsql_prepare");

		_FQsetResultError(conn, result);

		_FQrollbackTransaction(conn, trans);
		result->resultStatus = FBRES_FATAL_ERROR;

		_FQexecClearResult(result);

		return result;
	}

	/* If a temporary transaction was previously created, roll it back */
	if (temp_trans == true)
	{
		_FQrollbackTransaction(conn, trans);
		temp_trans = false;
	}

	/* Determine the statement's type */
	if (isc_dsql_sql_info(conn->status, &result->stmt_handle, sizeof (stmt_info), stmt_info, sizeof (info_buffer), info_buffer))
	{
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - isc_dsql_sql_info");

		_FQsetResultError(conn, result);

		_FQrollbackTransaction(conn, trans);
		result->resultStatus = FBRES_FATAL_ERROR;

		_FQexecClearResult(result);
		return result;
	}

	statement_type = _FQexecParseStatementType((char *) info_buffer);

	/* Query will not return rows */
	if (!result->sqlda_out->sqld)
	{
		/* Handle explicit SET TRANSACTION */
		if (statement_type == isc_info_sql_stmt_start_trans)
		{
			if (*trans != 0L)
			{
				_FQsetResultNonFatalError(conn, result, WARNING, "Currently in transaction");
				result->resultStatus = FBRES_EMPTY_QUERY;
			}
			else
			{
				_FQstartTransaction(conn, trans);
				conn->in_user_transaction = true;
				result->resultStatus = FBRES_TRANSACTION_START;
			}

			_FQexecClearResult(result);
			return result;
		}

		/* Handle explicit COMMIT */
		if (statement_type == isc_info_sql_stmt_commit)
		{
			 if (*trans == 0L)
			{
				_FQsetResultNonFatalError(conn, result, WARNING, "Not currently in transaction");
				result->resultStatus = FBRES_EMPTY_QUERY;
			}
			else
			{
				_FQcommitTransaction(conn, trans);
				result->resultStatus = FBRES_TRANSACTION_COMMIT;
			}

			 /* conn->in_user_transaction is only set if an explicit SET TRANSACTION
			  * command is passed to _FQexec */
			 if (conn->in_user_transaction == true)
				 conn->in_user_transaction = false;

			_FQexecClearResult(result);
			return result;
		}

		/* Handle explit ROLLBACK */
		if (statement_type == isc_info_sql_stmt_rollback)
		{
			if (*trans == 0L)
			{
				_FQsetResultNonFatalError(conn, result, WARNING, "Not currently in transaction");
				result->resultStatus = FBRES_EMPTY_QUERY;
			}
			else
			{
				_FQrollbackTransaction(conn, trans);
				result->resultStatus = FBRES_TRANSACTION_ROLLBACK;
			}

			/* conn->in_user_transaction is only set if an explicit SET TRANSACTION
			 * command is passed to _FQexec */
			if (conn->in_user_transaction == true)
				conn->in_user_transaction = false;
			_FQexecClearResult(result);
			return result;
		}

		/* Handle DDL statement */
		if (statement_type == isc_info_sql_stmt_ddl)
		{
			FQlog(conn, DEBUG1, "statement_type is DDL");

			temp_trans = false;
			if (*trans == 0L)
			{
				_FQstartTransaction(conn, trans);
				temp_trans = true;
			}

			if (isc_dsql_execute(conn->status, trans,  &result->stmt_handle, SQL_DIALECT_V6, NULL))
			{
				_FQrollbackTransaction(conn, trans);
				_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error executing DDL");
				_FQsetResultError(conn, result);

				result->resultStatus = FBRES_FATAL_ERROR;

				_FQexecClearResult(result);
				return result;
			}

			if ((conn->autocommit == true && conn->in_user_transaction == false) || temp_trans == true)
			{
				_FQcommitTransaction(conn, trans);
			}

			result->resultStatus = FBRES_COMMAND_OK;

			_FQexecClearResult(result);
			return result;
		}


		if (*trans == 0L)
		{
			_FQstartTransaction(conn, trans);

			if (conn->autocommit == false)
				conn->in_user_transaction = true;
		}

		if (isc_dsql_execute(conn->status, trans,  &result->stmt_handle, SQL_DIALECT_V6, NULL))
		{
			FQlog(conn, DEBUG1, "error executing non-SELECT");
			_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error executing non-SELECT");
			_FQsetResultError(conn, result);

			result->resultStatus = FBRES_FATAL_ERROR;
			_FQexecClearResult(result);
			return result;
		}

		if (conn->autocommit == true && conn->in_user_transaction == false)
		{
			_FQcommitTransaction(conn, trans);
		}

		result->resultStatus = FBRES_COMMAND_OK;
		_FQexecClearResult(result);
		return result;
	}

	/* begin transaction, if none set */

	if (*trans == 0L)
	{
		_FQstartTransaction(conn, trans);

		if (conn->autocommit == false)
			conn->in_user_transaction = true;
	}

	if (isc_dsql_describe(conn->status, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_out))
	{
		_FQsetResultError(conn, result);
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "isc_dsql_describe");

		result->resultStatus = FBRES_FATAL_ERROR;

		_FQexecClearResult(result);
		return result;
	}



	/* Expand sqlda to required number of columns */
	result->ncols = result->sqlda_out->sqld;

	if (result->sqlda_out->sqln < result->ncols) {

		free(result->sqlda_out);
		result->sqlda_out = (XSQLDA *) malloc(XSQLDA_LENGTH (result->ncols));
		memset(result->sqlda_out, '\0', XSQLDA_LENGTH (result->ncols));

		result->sqlda_out->version = SQLDA_VERSION1;
		result->sqlda_out->sqln = result->ncols;

		if (isc_dsql_describe(conn->status, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_out))
		{
			_FQsetResultError(conn, result);
			_FQsaveMessageField(&result, FB_DIAG_DEBUG, "isc_dsql_describe");

			result->resultStatus = FBRES_FATAL_ERROR;

			_FQexecClearResult(result);
			return result;
		}

		result->ncols = result->sqlda_out->sqld;
	}

	_FQexecInitOutputSQLDA(conn, result);

	if (isc_dsql_execute(conn->status, trans, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_out))
	{
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "isc_dsql_execute error");

		result->resultStatus = FBRES_FATAL_ERROR;
		_FQsetResultError(conn, result);

		/* if autocommit, and no explicit transaction set, rollback */
		if (conn->autocommit == true && conn->in_user_transaction == false)
		{
			_FQrollbackTransaction(conn, trans);
		}

		_FQexecClearResult(result);
		return result;
	}

	/* set up tuple holder */

	result->tuple_first = NULL;
	result->tuple_last = NULL;

	result->header = malloc(sizeof(FQresTupleAttDesc *) * result->ncols);

	while ((retcode = isc_dsql_fetch(conn->status, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_out)) == 0)
	{
		_FQstoreResult(result, conn, num_rows);
		num_rows++;
	}

	if (retcode != 100L)
	{
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "isc_dsql_fetch() error");
		result->resultStatus = FBRES_FATAL_ERROR;
		_FQsetResultError(conn, result);

		/* if autocommit, and no explicit transaction set, rollback */
		if (conn->autocommit == true && conn->in_user_transaction == false)
		{
			_FQrollbackTransaction(conn, trans);
		}

		_FQexecClearResult(result);
		return result;
	}

	result->resultStatus = FBRES_TUPLES_OK;
	result->ntups = num_rows;

	/* add an array of tuple pointers for offset-based access */
	_FQexecFillTuplesArray(result);

	/* if autocommit, and no explicit transaction set, commit */
	if (conn->autocommit == true && conn->in_user_transaction == false)
	{
		_FQcommitTransaction(conn, trans);
	}

	/* clear up internal storage */
	_FQexecClearResult(result);
	return result;
}


/**
 * FQexecParams()
 *
 * Execute a parameterized query.
 *
 * conn
 *   - a valid connection
 * stmt
 *   - a string containing the SQL to be executed
 * nParams
 *   - number of parameters supplied and should be the same as
 *     the length of the various arrays supplied (note: currently
 *     this argument is advisory and primarily for compatiblity
 *     with the libpq method PQexecParams(), however it may
 *     be used in the future)
 * paramTypes[]
 *   - (currently unused)
 * paramValues[]
 *   - actual query parameter values
 * paramLengths[]
 *   - (currently unused)
 * paramFormats[]
 *   - optional array to specify whether parameters are passed as
 *     strings (array entry is 0) or a text string to be converted
 *     to an RDB$DB_KEY value (array entry is -1). Binary formats
 *     may be supported in the future.
 * resultFormat
 *   - (currently unused)
 */
FBresult *
FQexecParams(FBconn *conn,
			 const char *stmt,
			 int nParams,
			 const int *paramTypes,
			 const char * const *paramValues,
			 const int *paramLengths,
			 const int *paramFormats,
			 int resultFormat)
{
	FBresult	 *result;

	if (!conn)
		return NULL;

	result = FQprepare(conn, stmt, nParams, paramTypes);

	if (result->resultStatus != FBRES_NO_ACTION)
		return result;

	return _FQexecParams(conn,
						 &conn->trans,
						 result,
						 true,
						 nParams,
						 paramValues,
						 paramLengths,
						 paramFormats,
						 resultFormat);
}


FBresult *
FQprepare(FBconn *conn,
		  const char *stmt,
		  int nParams,
		  const int *paramTypes)
{
	FBresult	 *result;
	bool		  temp_trans = false;
	isc_tr_handle *trans = &conn->trans;
	char		  info_buffer[20];
	static char	  stmt_info[] = { isc_info_sql_stmt_type };

	result = _FQinitResult(true);

	/* Allocate a statement. */
	if (isc_dsql_alloc_statement2(conn->status, &conn->db, &result->stmt_handle))
	{
		result->resultStatus = FBRES_FATAL_ERROR;
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - isc_dsql_allocate_statement");
		_FQsetResultError(conn, result);

		_FQexecClearResult(result);
		return result;
	}

	/* An active transaction is required to prepare the statement -
	 * if no transaction handle was provided by the caller,
	 * start a temporary transaction
	 */
	if (*trans == 0L)
	{
		_FQstartTransaction(conn, trans);
		temp_trans = true;
	}

	/* Prepare the statement. */
	if (isc_dsql_prepare(conn->status, trans, &result->stmt_handle, 0, stmt, SQL_DIALECT_V6, NULL))
	{
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - isc_dsql_prepare");

		_FQsetResultError(conn, result);

		_FQrollbackTransaction(conn, trans);

		result->resultStatus = FBRES_FATAL_ERROR;

		_FQexecClearResult(result);
		return result;
	}

	if (temp_trans == true)
	{
		_FQrollbackTransaction(conn, trans);
		temp_trans = false;
	}

	/* Determine the statement's type */
	if (isc_dsql_sql_info(conn->status, &result->stmt_handle, sizeof (stmt_info), stmt_info, sizeof (info_buffer), info_buffer))
	{
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - isc_dsql_sql_info");

		_FQsetResultError(conn, result);

		_FQrollbackTransaction(conn, trans);
		result->resultStatus = FBRES_FATAL_ERROR;

		_FQexecClearResult(result);
		return result;
	}

	result->statement_type = _FQexecParseStatementType((char *) info_buffer);

	FQlog(conn, DEBUG1, "statement_type: %i", result->statement_type);

	switch(result->statement_type)
	{
		case isc_info_sql_stmt_insert:
		case isc_info_sql_stmt_update:
		case isc_info_sql_stmt_delete:
		case isc_info_sql_stmt_select:
		case isc_info_sql_stmt_exec_procedure:
			/* INSERT ... RETURNING ... */
			break;

		default:
			_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - stmt type is not DML");

			_FQsetResultError(conn, result);

			_FQrollbackTransaction(conn, trans);
			result->resultStatus = FBRES_FATAL_ERROR;

			_FQexecClearResult(result);
			return result;
	}

	return result;
}

FBresult *
FQexecPrepared(FBconn *conn,
			   FBresult *result,
			   int nParams,
			   const char * const *paramValues,
			   const int *paramLengths,
			   const int *paramFormats,
			   int resultFormat)
{
	return _FQexecParams(conn,
						 &conn->trans,
						 result,
						 false,
						 nParams,
						 paramValues,
						 paramLengths,
						 paramFormats,
						 resultFormat);
}

/**
 * _FQexecParams()
 *
 * Actually execute the parameterized query. See above for parameter
 * details.
 *
 * Be warned, this was a pain to kludge together (oh Firebird C API, how
 * I love your cryptic minimalism) and is in dire need of refactoring.
 * But it works. Mostly.
 */
static FBresult *
_FQexecParams(FBconn *conn,
			  isc_tr_handle *trans,
			  FBresult	 *result,
			  bool free_result_stmt_handle,
			  int nParams,
			  const char * const *paramValues,
			  const int *paramLengths,
			  const int *paramFormats,
			  int resultFormat
	)
{
	XSQLVAR		 *var;
	int			  i;

	ISC_STATUS    retcode;
	int			  exec_result;
	char		  error_message[1024];


	if (isc_dsql_describe_bind(conn->status, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_in))
	{
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - isc_dsql_describe_bind");
		_FQsetResultError(conn, result);
		result->resultStatus = FBRES_FATAL_ERROR;

		_FQrollbackTransaction(conn, trans);

		_FQexecClearResult(result);
		return result;
	}

	if (*trans == 0L)
	{
		FQlog(conn, DEBUG1, "_FQexecParams: starting transaction...");
		_FQstartTransaction(conn, trans);

		if (conn->autocommit == false)
			conn->in_user_transaction = true;
	}

	if (result->sqlda_in->sqld > result->sqlda_in->sqln)
	{
		int sqln = result->sqlda_in->sqld;

		free(result->sqlda_in);
		result->sqlda_in = (XSQLDA *)malloc(XSQLDA_LENGTH(sqln));
		memset(result->sqlda_in, '\0', XSQLDA_LENGTH(sqln));
		result->sqlda_in->sqln = sqln;
		result->sqlda_in->version = SQLDA_VERSION1;
		isc_dsql_describe_bind(conn->status, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_in);

		FQlog(conn, DEBUG1, "%lu; sqln now %i %i", XSQLDA_LENGTH(sqln), sqln, result->sqlda_in->sqld );
	}

	/* from dbdimp.c - not sure what it's about, but note here
	 * in case we encounter a similiar issue */
	/* workaround for date problem (bug #429820)
	if (dtype == SQL_TEXT)
	{
		if (ivar->sqlsubtype == 0x77)
			dtype = SQL_TIMESTAMP;
	}
	*/

	FQlog(conn, DEBUG1, "_FQexecParams: sqld %i", result->sqlda_in->sqld);

	for (i = 0, var = result->sqlda_in->sqlvar; i < result->sqlda_in->sqld; i++, var++)
	{
		int dtype = (var->sqltype & ~1); /* drop flag bit for now */

		int len = 0;

		FQlog(conn, DEBUG1, "_FQexecParams: here %i", i);

		var->sqldata = NULL;
		var->sqllen = 0;

		if (paramFormats != NULL)
			FQlog(conn, DEBUG1, "%i: %s", i, paramValues[i]);

		/* For NULL values, initialise empty sqldata/sqllen */
		if (paramValues[i] == NULL)
		{
			int size = -1;

			switch(dtype)
			{
				case SQL_SHORT:
					size = sizeof(ISC_SHORT);
					break;

				case SQL_LONG:
					size = sizeof(ISC_LONG);
					break;

				case SQL_INT64:
					size = sizeof(ISC_INT64);
					break;
#if defined SQL_INT128
				/* Firebird 4.0 and later */
				case SQL_INT128:
					size = sizeof(__int128);
					break;
#endif
				case SQL_FLOAT:
					size = sizeof(float);
					break;

				case SQL_DOUBLE:
					size = sizeof(double);
					break;

				case SQL_VARYING:
					size = 0;
					break;

				case SQL_TEXT:
					size = 0;
					break;

				case SQL_TYPE_TIME:
					size = sizeof(ISC_TIME);
					break;
#ifdef HAVE_TIMEZONE
				case SQL_TIME_TZ:
					size = sizeof(ISC_TIME_TZ);
					break;

				case SQL_TIME_TZ_EX:
					size = sizeof(ISC_TIME_TZ_EX);
					break;
#endif

				case SQL_TIMESTAMP:
					size = sizeof(ISC_TIMESTAMP);
					break;
#ifdef HAVE_TIMEZONE
				case SQL_TIMESTAMP_TZ:
					size = sizeof(ISC_TIMESTAMP_TZ);
					break;

				case SQL_TIMESTAMP_TZ_EX:
					size = sizeof(ISC_TIMESTAMP_TZ_EX);
					break;
#endif
				case SQL_TYPE_DATE:
					size = sizeof(ISC_DATE);
					break;


				case SQL_BLOB:
					size = sizeof(ISC_QUAD);
					break;

#if defined SQL_BOOLEAN
				/* Firebird 3.0 and later */
				case SQL_BOOLEAN:
					size = sizeof(FB_BOOLEAN);
					break;
#endif

				default:
					sprintf(error_message, "Unhandled sqlda_in type: %i", dtype);

					_FQsetResultError(conn, result);
					_FQsaveMessageField(&result, FB_DIAG_DEBUG, error_message);

					result->resultStatus = FBRES_FATAL_ERROR;

					_FQexecClearResult(result);
			}

			/* var->sqldata remains NULL to indicate NULL */
			if (size >= 0)
				var->sqllen = size;
		}
		else
		{
			switch(dtype)
			{
				case SQL_SHORT:
				case SQL_LONG:
				{
					char format[64];
					long p, q, r, result;
					const char *svalue;

					p = q = r = (long) 0;
					svalue = paramValues[i];
					len = strlen(svalue);

					/* with decimals? */
					if (var->sqlscale < 0)
					{
						/* NUMERIC(?,?) */
						int	 scale = (int) (pow(10.0, (double) -var->sqlscale));
						int	 dscale;
						char *tmp;
						char *neg;

						FQlog(conn, DEBUG1, "sqlscale < 0; scale is %i", scale);

						sprintf(format, "%%ld.%%%dld%%1ld", -var->sqlscale);

						/* negative -0.x hack */
						neg = strchr(svalue, '-');
						if (neg)
						{
							svalue = neg + 1;
							len = strlen(svalue);
						}

						if (!sscanf(svalue, format, &p, &q, &r))
						{
							/* here we handle values such as .78 passed as string */
							sprintf(format, ".%%%dld%%1ld", -var->sqlscale);
							if (!sscanf(svalue, format, &q, &r) )
								FQlog(conn, DEBUG1, "problem parsing SQL_SHORT/SQL_LONG type");
						}

						/* Round up if r is 5 or greater */
						if (r >= 5)
						{
							q++;			/* round q up by one */
							p += q / scale; /* round p up by one if q overflows */
							q %= scale;		/* modulus if q overflows */
						}

						/* decimal scaling */
						tmp	   = strchr(svalue, '.');
						dscale = (tmp)
							? -var->sqlscale - (len - (int) (tmp - svalue)) + 1
							: 0;

						if (dscale < 0) dscale = 0;

						/* final result */
						result = (long) (p * scale + q * (int) (pow(10.0, (double) dscale))) * (neg ? -1 : 1);
						FQlog(conn, DEBUG1, "SQL_SHORT/LONG: decimal result is %li", result);
					}
					else
					{
						/* numeric(?,0): scan for one decimal and do rounding*/

						sprintf(format, "%%ld.%%1ld");

						if (!sscanf(svalue, format, &p, &r))
						{
							sprintf(format, ".%%1ld");
							if (!sscanf(svalue, format, &r))
								FQlog(conn, DEBUG1, "problem parsing SQL_SHORT/SQL_LONG type");
						}

						/* rounding */
						if (r >= 5)
						{
							if (p < 0) p--; else p++;
						}

						result = (long) p;
					}

					if (dtype == SQL_SHORT)
					{
						var->sqldata = (char *)malloc(sizeof(ISC_SHORT));
						var->sqllen = sizeof(ISC_SHORT);
						*(ISC_SHORT *) (var->sqldata) = (ISC_SHORT) result;
					}
					else
					{
						var->sqldata = (char *)malloc(sizeof(ISC_LONG));
						var->sqllen = sizeof(ISC_LONG);
						*(ISC_LONG *) (var->sqldata) = (ISC_LONG) result;
					}

					break;
				}

				case SQL_INT64:
				{
					const char	   *svalue;
					char	 format[64];
					ISC_INT64 p, q, r;

					FQlog(conn, DEBUG1, "INT64");
					var->sqldata = (char *)malloc(sizeof(ISC_INT64));
					memset(var->sqldata, '\0', sizeof(ISC_INT64));

					p = q = r = (ISC_INT64) 0;
					svalue = paramValues[i];
					len = strlen(svalue);

					/* with decimals? */
					if (var->sqlscale < 0)
					{
						/* numeric(?,?) */
						int	 scale = (int) (pow(10.0, (double) -var->sqlscale));
						int	 dscale;
						char *tmp;
						char *neg;

						sprintf(format, S_INT64_FULL, -var->sqlscale);

						/* negative -0.x hack */
						neg = strchr(svalue, '-');
						if (neg)
						{
							svalue = neg + 1;
							len = strlen(svalue);
						}

						if (!sscanf(svalue, format, &p, &q, &r))
						{
							/* here we handle values such as .78 passed as string */
							sprintf(format, S_INT64_DEC_FULL, -var->sqlscale);
							if (!sscanf(svalue, format, &q, &r))
								FQlog(conn, DEBUG1, "problem parsing SQL_INT64 type");
						}

						/* Round up if r is 5 or greater */
						if (r >= 5)
						{
							q++;			/* round q up by one */
							p += q / scale; /* round p up by one if q overflows */
							q %= scale;		/* modulus if q overflows */
						}

						/* decimal scaling */
						tmp	   = strchr(svalue, '.');
						dscale = (tmp)
							? -var->sqlscale - (len - (int) (tmp - svalue)) + 1
							: 0;

						if (dscale < 0)
							dscale = 0;

						*(ISC_INT64 *) (var->sqldata) = (ISC_INT64) (p * scale + q * (int) (pow(10.0, (double) dscale))) * (neg? -1: 1);
						var->sqllen = sizeof(ISC_INT64);
					}
					else
					{
						/* NUMERIC(?,0): scan for one decimal and do rounding */

						sprintf(format, S_INT64_NOSCALE);

						if (!sscanf(svalue, format, &p, &r))
						{
							sprintf(format, S_INT64_DEC_NOSCALE);
							if (!sscanf(svalue, format, &r))
								FQlog(conn, DEBUG1, "problem parsing SQL_INT64 type");
						}

						/* rounding */
						if (r >= 5)
						{
							if (p < 0) p--; else p++;
						}

						*(ISC_INT64 *) (var->sqldata) = (ISC_INT64) p;
						var->sqllen = sizeof(ISC_INT64);
					}

					break;
				}

#if defined SQL_INT128
				/* Firebird 4.0 and later */
				case SQL_INT128:
					var->sqldata = (char *)malloc(sizeof(__int128));
					memset(var->sqldata, '\0', sizeof(__int128));
					*(__int128 *) (var->sqldata) = convert_int128(paramValues[i]);
					var->sqllen = sizeof(__int128);
					break;
#endif
				case SQL_FLOAT:
					var->sqldata = (char *)malloc(sizeof(float));
					var->sqllen = sizeof(float);
					*(float *)(var->sqldata) = (float)atof(paramValues[i]);
					break;

				case SQL_DOUBLE:
					var->sqldata = (char *)malloc(sizeof(double));
					var->sqllen = sizeof(double);
					*(double *) (var->sqldata) = atof(paramValues[i]);
					break;

				case SQL_VARYING:
					var->sqltype = SQL_TEXT; /* need this */
					len = strlen(paramValues[i]);

					var->sqllen = len; /* need this */
					var->sqldata = (char *)malloc(sizeof(char)*var->sqllen);
					memcpy(var->sqldata, paramValues[i], len);
					break;

				case SQL_TEXT:

					/* convert RDB$DB_KEY hex value to raw bytes if requested */
					if (paramFormats != NULL && paramFormats[i] == -1)
					{
						unsigned char *sqlptr;
						unsigned char *srcptr;
						unsigned char *srcptr_ix;
						unsigned char *srcptr_parsed;
						int ix = 0;

						srcptr = (unsigned char *)_FQdeparseDbKey(paramValues[i]);

						srcptr_parsed = (unsigned char *)_FQparseDbKey((char *)srcptr);
						FQlog(conn, DEBUG1, "srcptr %s", srcptr_parsed);
						free(srcptr_parsed);

						len = 8;
						var->sqllen = len;
						var->sqldata = (char *)malloc(len);

						sqlptr = (unsigned char *)var->sqldata ;
						srcptr_ix = srcptr;

						for (ix = 0; ix < len; ix++)
						{
							*sqlptr++ = *srcptr_ix++;
						}

						free(srcptr);
					}
					else
					{
						len = strlen(paramValues[i]);
						var->sqldata = (char *)malloc(sizeof(char) * len);
						var->sqllen = len;
						memcpy(var->sqldata, paramValues[i], len);
					}

					break;

				case SQL_TYPE_TIME:
#ifdef HAVE_TIMEZONE
				case SQL_TIME_TZ:
				case SQL_TIME_TZ_EX:
#endif
				case SQL_TIMESTAMP:
#ifdef HAVE_TIMEZONE
				case SQL_TIMESTAMP_TZ:
				case SQL_TIMESTAMP_TZ_EX:
#endif
				case SQL_TYPE_DATE:
					/* Here we coerce the time-related column types to CHAR,
					 * causing Firebird to use its internal parsing mechanisms
					 * to interpret the supplied literal
					 */
					len = strlen(paramValues[i]);
					/* From dbimp.c: "workaround for date problem (bug #429820)" */
					var->sqltype = SQL_TEXT;
					var->sqlsubtype = 0x77;
					var->sqllen = len;
					var->sqldata = (char *)malloc(sizeof(char)*len);
					memcpy(var->sqldata, paramValues[i], len);

					break;

				case SQL_BLOB:
				{
					/* must be initialised to 0 */
					isc_blob_handle blob_handle = 0;
					char *ptr = (char *)paramValues[i];

					len = strlen(paramValues[i]);
					var->sqldata = (char *)malloc(sizeof(ISC_QUAD));
					var->sqllen = sizeof(ISC_QUAD);

					isc_create_blob2(
						conn->status,
						&conn->db,
						&conn->trans,
						&blob_handle,
						(ISC_QUAD *)var->sqldata,
						0,		 /* Blob Parameter Buffer length = 0; no filter will be used */
						NULL	 /* NULL Blob Parameter Buffer, since no filter will be used */
						);
					while (ptr < paramValues[i] + len)
					{
						int seg_len = BLOB_SEGMENT_LEN;

						if (ptr + seg_len > (paramValues[i] + len))
						{
							seg_len = (paramValues[i] + len) - ptr;
						}

						isc_put_segment(
							conn->status,
							&blob_handle,
							seg_len,
							ptr);

						ptr += BLOB_SEGMENT_LEN;
					}
					isc_close_blob(conn->status, &blob_handle);
					break;
				}

#if defined SQL_BOOLEAN
				/* Firebird 3.0 and later */
				case SQL_BOOLEAN:
					var->sqldata = (char *)malloc(sizeof(FB_BOOLEAN));
					var->sqllen = sizeof(FB_BOOLEAN);

					if (strncasecmp(paramValues[i], "0", 1) == 0)
						*var->sqldata = FB_FALSE;
					else if (strncasecmp(paramValues[i], "1", 1) == 0)
						*var->sqldata = FB_TRUE;
					else if (strncasecmp(paramValues[i], "false", 5) == 0)
						*var->sqldata = FB_FALSE;
					else if (strncasecmp(paramValues[i], "f", 1) == 0)
						*var->sqldata = FB_FALSE;
					else if (strncasecmp(paramValues[i], "true", 4) == 0)
						*var->sqldata = FB_TRUE;
					else if (strncasecmp(paramValues[i], "t", 1) == 0)
						*var->sqldata = FB_TRUE;
					else
						*var->sqldata = FB_FALSE;

					break;
#endif


				default:
					sprintf(error_message, "Unhandled sqlda_in type: %i", dtype);

					_FQsetResultError(conn, result);
					_FQsaveMessageField(&result, FB_DIAG_DEBUG, error_message);

					result->resultStatus = FBRES_FATAL_ERROR;

					_FQexecClearResult(result);
					return result;
			}
		}

		if (var->sqltype & 1)
		{
			/* allocate variable to hold NULL status */

			var->sqlind = (short *)malloc(sizeof(short));
			*(short *)var->sqlind = (paramValues[i] == NULL) ? -1 : 0;
		}
	}

	if (isc_dsql_describe(conn->status, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_out))
	{
		_FQsetResultError(conn, result);
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "isc_dsql_describe");

		result->resultStatus = FBRES_FATAL_ERROR;
		_FQexecClearResult(result);
		return result;
	}

	/* Expand output sqlda to required number of columns */
	result->ncols = result->sqlda_out->sqld;

	FQlog(conn, DEBUG2, "_FQexecParams(): ncols is %i", result->ncols);

	/* No output expected */
	if (!result->ncols)
	{
		if (isc_dsql_execute(conn->status, trans, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_in))
		{
			FQlog(conn, DEBUG1, "isc_dsql_execute(): error");

			_FQsaveMessageField(&result, FB_DIAG_DEBUG, "isc_dsql_execute() error");

			_FQsetResultError(conn, result);
			result->resultStatus = FBRES_FATAL_ERROR;

			/* if autocommit, and no explicit transaction set, rollback */
			if (conn->autocommit == true && conn->in_user_transaction == false)
			{
				_FQrollbackTransaction(conn, trans);
			}

			_FQexecClearResultParams(conn, result, free_result_stmt_handle);

			return result;
		}

		FQlog(conn, DEBUG1, "_FQexecParams(): finished non-SELECT with no rows to return");
		result->resultStatus = FBRES_COMMAND_OK;
		if (conn->autocommit == true && conn->in_user_transaction == false)
		{
			FQlog(conn, DEBUG1, "committing...");
			_FQcommitTransaction(conn, trans);
		}

		_FQexecClearResultParams(conn, result, free_result_stmt_handle);

		return result;
	}

	if (result->sqlda_out->sqln < result->ncols) {
		free(result->sqlda_out);
		result->sqlda_out = (XSQLDA *) malloc(XSQLDA_LENGTH (result->ncols));
		memset(result->sqlda_out, '\0', XSQLDA_LENGTH (result->ncols));

		result->sqlda_out->version = SQLDA_VERSION1;
		result->sqlda_out->sqln = result->ncols;

		isc_dsql_describe(conn->status, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_out);

		result->ncols = result->sqlda_out->sqld;
	}

	_FQexecInitOutputSQLDA(conn, result);

	/* "isc_info_sql_stmt_exec_procedure" also covers "RETURNING ..." statements */
	if (result->statement_type == isc_info_sql_stmt_exec_procedure)
		exec_result = isc_dsql_execute2(conn->status, trans, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_in, result->sqlda_out);
	else
		exec_result = isc_dsql_execute(conn->status, trans, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_in);

	if (exec_result)
	{
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "isc_dsql_execute2() error");

		result->resultStatus = FBRES_FATAL_ERROR;
		_FQsetResultError(conn, result);

		/* if autocommit, and no explicit transaction set, rollback */
		if (conn->autocommit == true && conn->in_user_transaction == false)
		{
			_FQrollbackTransaction(conn, trans);
		}

		_FQexecClearResult(result);

		if (free_result_stmt_handle)
			isc_dsql_free_statement(conn->status, &result->stmt_handle, DSQL_drop);

		return result;
	}

	/* set up tuple holder */
	result->tuple_first = NULL;
	result->tuple_last = NULL;

	result->header = malloc(sizeof(FQresTupleAttDesc *) * result->ncols);

	/* XXX TODO: only needed for "SELECT ... FOR UPDATE " */
	if (0 && isc_dsql_set_cursor_name(conn->status, &result->stmt_handle, "dyn_cursor", 0))
	{
		_FQsetResultError(conn, result);
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, error_message);

		result->resultStatus = FBRES_FATAL_ERROR;

		_FQexecClearResultParams(conn, result, free_result_stmt_handle);

		return result;
	}

	if (result->statement_type == isc_info_sql_stmt_exec_procedure)
	{
		_FQstoreResult(result, conn, 0);
		result->ntups = 1;
	}
	else
	{
		int num_rows = 0;

		while ((retcode = isc_dsql_fetch(conn->status, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_out)) == 0)
		{
			_FQstoreResult(result, conn, num_rows);
			num_rows ++;
		}

		if (retcode != 100L)
		{
			_FQsaveMessageField(&result, FB_DIAG_DEBUG, "isc_dsql_fetch() error");

			result->resultStatus = FBRES_FATAL_ERROR;
			_FQsetResultError(conn, result);

			/* if autocommit, and no explicit transaction set, rollback */
			if (conn->autocommit == true && conn->in_user_transaction == false)
			{
				_FQrollbackTransaction(conn, trans);
			}

			_FQexecClearResult(result);

			if (free_result_stmt_handle)
				isc_dsql_free_statement(conn->status, &result->stmt_handle, DSQL_drop);

			return result;
		}

		result->ntups = num_rows;
	}

	/*
	 * HACK: INSERT/UPDATE/DELETE ... RETURNING ... sometimes results in a
	 * "request synchronization error" - ignoring this doesn't seem to
	 * cause any problems. Potentially this is related to issues with cursor
	 * usage.
	 *
	 * See maybe: http://support.codegear.com/article/35153
	 */
	if (0 && retcode != 100L && retcode != isc_req_sync)
	{
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - isc_dsql_fetch reported %lu", retcode);

		_FQsetResultError(conn, result);

		_FQrollbackTransaction(conn, trans);
		result->resultStatus = FBRES_FATAL_ERROR;

		_FQexecClearResultParams(conn, result, free_result_stmt_handle);

		return result;
	}

	if (free_result_stmt_handle)
	{
		if (isc_dsql_free_statement(conn->status, &result->stmt_handle, DSQL_drop))
		{
			_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - isc_dsql_free_statement");

			_FQsetResultError(conn, result);

			_FQrollbackTransaction(conn, trans);

			result->resultStatus = FBRES_FATAL_ERROR;

			return result;
		}
	}

	/* add an array for offset-based access */
	_FQexecFillTuplesArray(result);

	result->resultStatus = FBRES_TUPLES_OK;

	/* if autocommit, and no explicit transaction set, commit */
	if (conn->autocommit == true && conn->in_user_transaction == false)
	{
		_FQcommitTransaction(conn, trans);
	}

	/*
	 * Clear up internal storage; we already freed the statement handle,
	 * if required.
	 */
	_FQexecClearResult(result);

	return result;
}

static void
_FQstoreResult(FBresult *result, FBconn *conn, int num_rows)
{
	FQresTuple *tuple_next = (FQresTuple *)malloc(sizeof(FQresTuple));
	int i;

	tuple_next->position = num_rows;
	tuple_next->max_lines = 1;
	tuple_next->next = NULL;
	tuple_next->values = malloc(sizeof(FQresTupleAtt *) * result->ncols);

	/* store header information */
	if (num_rows == 0)
	{
		for (i = 0; i < result->ncols; i++)
		{
			FQresTupleAttDesc *desc = (FQresTupleAttDesc *)malloc(sizeof(FQresTupleAttDesc));
			XSQLVAR *var1 = &result->sqlda_out->sqlvar[i];

			desc->desc_len = var1->sqlname_length;
			desc->desc = (char *)malloc(desc->desc_len + 1);
			memcpy(desc->desc, var1->sqlname, desc->desc_len + 1);
			desc->desc_dsplen = FQdspstrlen(desc->desc, FQclientEncodingId(conn));

			if (var1->aliasname_length == var1->sqlname_length
				&& strncmp(var1->aliasname, var1->sqlname, var1->aliasname_length ) == 0)
			{
				desc->alias_len = 0;
				desc->alias = NULL;
			}
			else
			{
				desc->alias_len = var1->aliasname_length;
				desc->alias = (char *)malloc(desc->alias_len + 1);
				memcpy(desc->alias, var1->aliasname, desc->alias_len + 1);
				desc->alias_dsplen = FQdspstrlen(desc->alias, FQclientEncodingId(conn));
			}

			/* store table name, if set */
			if (var1->relname_length)
			{
				desc->relname_len = var1->relname_length;
				desc->relname = (char *)malloc(desc->relname_len + 1);
				memset(desc->relname, '\0', desc->relname_len + 1);
				strncpy(desc->relname, var1->relname, desc->relname_len);
			}
			else
			{
				desc->relname_len = 0;
				desc->relname = NULL;
			}

			desc->att_max_len = 0;
			desc->att_max_line_len = 0;

			/* Firebird returns RDB$DB_KEY as "DB_KEY" - set the pseudo-datatype */
			if (strncmp(desc->desc, "DB_KEY", 6) == 0 && strlen(desc->desc) == 6)
				desc->type = SQL_DB_KEY;
			else
				desc->type = var1->sqltype & ~1;

			desc->has_null = false;
			result->header[i] = desc;
		}
	}

	/* Store tuple data */
	for (i = 0; i < result->ncols; i++)
	{
		XSQLVAR *var = (XSQLVAR *)&result->sqlda_out->sqlvar[i];
		FQresTupleAtt *tuple_att = _FQformatDatum(conn, result->header[i], var);

		if (tuple_att->lines > tuple_next->max_lines)
		{
			tuple_next->max_lines = tuple_att->lines;
		}

		if (tuple_att->value == NULL)
		{
			result->header[i]->has_null = true;
		}
		else
		{
			/* TODO: set max lines */

			if (tuple_att->dsplen > result->header[i]->att_max_len)
			{
				result->header[i]->att_max_len = tuple_att->dsplen;
			}

			if (tuple_att->dsplen_line > result->header[i]->att_max_line_len)
			{
				result->header[i]->att_max_line_len = tuple_att->dsplen_line;
			}
		}

		tuple_next->values[i] = tuple_att;
	}

	if (result->tuple_first == NULL)
	{
		result->tuple_first = tuple_next;
		result->tuple_last = result->tuple_first;
	}
	else
	{
		result->tuple_last->next = tuple_next;
		result->tuple_last = tuple_next;
	}
}


/**
 * FQdeallocatePrepared()
 *
 * Essentially a wrapper around isc_dsql_free_statement(); call after
 * finishing with FQexecPrepared().
 */

void
FQdeallocatePrepared(FBconn *conn, FBresult *result)
{
	isc_dsql_free_statement(conn->status, &result->stmt_handle, DSQL_drop);
}

/**
 * FQexecTransaction()
 *
 * Convenience function to execute a query using the internal
 * transaction handle.
 */
FBresult *
FQexecTransaction(FBconn *conn, const char *stmt)
{
	FBresult	  *result = NULL;

	if (!conn)
	{
		result->resultStatus = FBRES_FATAL_ERROR;

		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - invalid connection object");
		_FQsetResultError(conn, result);

		return result;
	}

	if (_FQstartTransaction(conn, &conn->trans_internal) == TRANS_ERROR)
	{
		result->resultStatus = FBRES_FATAL_ERROR;

		/* XXX todo: set error */
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "transaction error");
		isc_print_status(conn->status);

		return result;
	}

	result = _FQexec(conn, &conn->trans_internal, stmt);

	if (FQresultStatus(result) == FBRES_FATAL_ERROR)
	{
		/* XXX todo: set error */
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "query execution error");
		isc_print_status(conn->status);
		_FQrollbackTransaction(conn, &conn->trans_internal);
	}
	/* Non-select query */
	else if (FQresultStatus(result) == FBRES_COMMAND_OK)
	{
		// TODO: show some meaningful output?
		if (_FQcommitTransaction(conn, &conn->trans_internal) == TRANS_ERROR)
		{
			/* XXX todo: set error */
			_FQsaveMessageField(&result, FB_DIAG_DEBUG, "transaction commit error");
			isc_print_status(conn->status);
			_FQrollbackTransaction(conn, &conn->trans_internal);
		}
	}
	/* Query returning rows */
	else if (FQresultStatus(result) == FBRES_TUPLES_OK)
	{
		_FQcommitTransaction(conn, &conn->trans_internal);
	}

	return result;
}


/*
 * =========================
 * Result handling functions
 * =========================
 */

/**
 * FQresultStatus()
 *
 * Returns the result status of the previously execute command.
 *
 * TODO: return something else if res is NULL?
 */
FQexecStatusType
FQresultStatus(const FBresult *res)
{
	if (!res)
		return FBRES_FATAL_ERROR;

	return res->resultStatus;
}


/**
 * FQresStatus()
 *
 * Converts the enumerated type returned by FQresultStatus into a
 * string constant describing the status code.
 */
char *
FQresStatus(FQexecStatusType status)
{
	if ((unsigned int) status >= sizeof fbresStatus / sizeof fbresStatus[0])
		return "invalid FQexecStatusType code";

	return fbresStatus[status];
}


/**
 * FQsqlCode()
 *
 * Returns the Firebird SQL code associated with the query result.
 *
 * See here for a full list:
 *
 *     https://firebirdsql.org/file/documentation/reference_manuals/fblangref25-en/html/fblangref25-appx02-sqlcodes.html
 *     http://ibexpert.net/ibe/index.php?n=Doc.Firebird21ErrorCodes
 *
 * Following additional codes defined by libfq:
 *
 * -1 = query OK
 * -2 = no result
 *
 * TODO: return GDS code if feasible. FB docs say:
 * "SQLCODE has been used for many years and should be considered as deprecated now.
 *  Support for SQLCODE is likely to be dropped in a future version."
 */

int
FQsqlCode(const FBresult *res)
{
	if (res == NULL)
		return -2;

	return (int)res->fbSQLCODE;
}


/**
 * FQntuples()
 *
 * Returns the number of tuples (rows) in the provided result.
 * Defaults to -1 if no query has been executed.
 */
int
FQntuples(const FBresult *res)
{
	if (!res)
		return -1;

	return res->ntups;
}


/**
 * FQnfields()
 *
 * Returns the number of columns (fields) in the provided result.
 * Defaults to -1 if no query has been executed.
 */
int
FQnfields(const FBresult *res)
{
	if (!res)
		return -1;

	return res->ncols;
}

static int
check_tuple_field_number(const FBresult *res,
						 int row_number, int column_number)
{
	if (!res)
		return false;

	if (row_number < 0 || row_number >= res->ntups)
		return false;

	if (column_number < 0 || column_number >= res->ncols)
		return false;

	return true;
}


/**
 * FQgetvalue()
 *
 * Returns a single field of an FBresult.
 *
 * Row and column numbers start at 0.
 *
 * NOTE: this function will return NULL if invalid row/column parameters
 *   are provided, as well as when the tuple value is actually NULL.
 *   To determine if a tuple value is null, use FQgetisnull().
 *
 */
char *
FQgetvalue(const FBresult *res,
           int row_number,
           int column_number)
{
	if (!check_tuple_field_number(res, row_number, column_number))
		return NULL;

	return res->tuples[row_number]->values[column_number]->value;
}


/**
 * FQgetisnull()
 *
 * Tests a field for a null value. Row and column numbers start at 0.
 * This function returns 1 if the field is null and 0 if it contains a non-null value.
 *
 * Note that libpq's PQgetvalue() returns an empty string if the field contains a
 * NULL value; FQgetvalue() returns NULL, but will also return NULL if
 * invalid parameters are provided, so FQgetisnull() will provide a
 * definitive result.
 */
int
FQgetisnull(const FBresult *res,
            int row_number,
            int column_number)
{
	if (!check_tuple_field_number(res, row_number, column_number))
		return 1;				/* pretend it is null */

	if (res->tuples[row_number]->values[column_number]->has_null == true)
		return 1;

	return 0;
}


/**
 * FQgetlines()
 *
 * Return max number of lines in column
 */
int
FQgetlines(const FBresult *res,
			int row_number,
			int column_number)
{
	if (!check_tuple_field_number(res, row_number, column_number))
		return -1;

	return res->tuples[row_number]->values[column_number]->lines;
}


/**
 * FQrgetlines()
 *
 * Return max number of lines in row
 */
int
FQrgetlines(const FBresult *res, int row_number)
{
	if (!res)
		return -1;

	if (row_number < 0 || row_number >= res->ntups)
		return -1;

	return res->tuples[row_number]->max_lines;
}


/**
 * FQfhasNull()
 *
 * Determine if for the provided column number, the result set contains
 * at least one NULL.
 *
 * Not to be confused with FQgetisnull(), which determines the NULL status
 * for the given column of a particular result tuple.
 */
bool
FQfhasNull(const FBresult *res, int column_number)
{
	if (!res)
		return false;

	if (column_number < 0 || column_number >= res->ncols)
		return false;

	return res->header[column_number]->has_null;
}


/**
 * FQfmaxwidth()
 *
 * Provides the maximum width of a column in single character units.
 *
 */
int
FQfmaxwidth(const FBresult *res, int column_number)
{
	int max_width;

	if (!res)
		return -1;

	if (column_number < 0 || column_number >= res->ncols)
		return -1;

	if (!res->header)
		return -1;

	if (res->header[column_number]->alias_len)
		max_width = res->header[column_number]->att_max_len > res->header[column_number]->alias_dsplen
			? res->header[column_number]->att_max_line_len
			: res->header[column_number]->alias_dsplen;
	else
		max_width = res->header[column_number]->att_max_len > res->header[column_number]->desc_dsplen
			? res->header[column_number]->att_max_line_len
			: res->header[column_number]->desc_dsplen;

	return max_width;
}


/**
 * FQfname()
 *
 * Provides the name (or alias, if set) of a particular field (column).
 */
char *
FQfname(const FBresult *res, int column_number)
{
	if (!res)
		return NULL;

	if (column_number < 0 || column_number >= res->ncols)
		return NULL;

	if (!res->header)
		return NULL;

	/* return alias, if set */
	if (res->header[column_number]->alias_len)
		return res->header[column_number]->alias;

	return res->header[column_number]->desc;
}


/**
 * FQgetlength()
 *
 * Get length in bytes of a particular tuple column.
 */
int
FQgetlength(const FBresult *res,
            int row_number,
            int column_number)
{
	if (!check_tuple_field_number(res, row_number, column_number))
		return -1;

	return res->tuples[row_number]->values[column_number]->len;
}


/**
 * FQgetdsplen()
 *
 * Returns the display length in single characters of the specified FBresult
 * field.
 *
 * Row and column numbers start at 0.
 */
int
FQgetdsplen(const FBresult *res,
            int row_number,
            int column_number)
{

	if (!check_tuple_field_number(res, row_number, column_number))
		return -1;

	return res->tuples[row_number]->values[column_number]->dsplen;
}


/**
 * FQfformat()
 *
 * Returns the format code indicating the format of the given column:
 *
 *  0 - text
 *  1 - binary
 * -1 - invalid column specification
 *
 * Column numbers start at 0.
 *
 * TODO: define enum/constants for these
 */
short
FQfformat(const FBresult *res, int column_number)
{
	if (!res)
		return -1;

	if (column_number < 0 || column_number >= res->ncols)
		return -1;

	switch(FQftype(res, column_number))
	{
		/* TODO: differentiate BLOB types */
		case SQL_BLOB:
			return 1;
	}

	return 0;
}


/**
 * FQftype()
 *
 * Returns the data type associated with the given column number.
 *
 * The data type will be an integer corresponding to one of the SQL_*
 * constants defined in ibase.h (and repeated in libfq.h), extended with
 * the following pseudo-types for convenience:
 *
 *  - SQL_INVALID_TYPE
 *  - SQL_DB_KEY
 *
 * Column numbers start at 0.
 */
short
FQftype(const FBresult *res, int column_number)
{
	if (!res)
		return SQL_INVALID_TYPE;

	if (column_number < 0 || column_number >= res->ncols)
		return SQL_INVALID_TYPE;

	return res->header[column_number]->type;
}


/*
 * ========================
 * Error handling functions
 * ========================
 */

/**
 * FQerrorMessage()
 *
 * Returns the most recent error message associated with the result, or an
 * empty string.
 */
char *
FQerrorMessage(const FBconn *conn)
{
	if (conn == NULL)
		return "";

	return conn->errMsg == NULL ? "" : conn->errMsg;
}


/**
 * FQresultErrorMessage()
 *
 * Returns the error message associated with the result, or an empty string.
 */
char *
FQresultErrorMessage(const FBresult *result)
{
	if (result == NULL)
		return "";

	return result->errMsg == NULL ? "" : result->errMsg;
}


/**
 * FQresultErrorField()
 *
 * Returns an individual field of an error report, or NULL.
 */
char *
FQresultErrorField(const FBresult *res, FQdiagType fieldcode)
{
	FBMessageField *mfield;

	if (!res || !res->errFields)
		return NULL;

	for (mfield = res->errFields; mfield != NULL; mfield = mfield->next)
	{
		if (mfield->code == fieldcode)
		{
			return mfield->value;
		}
	}

	return NULL;
}


/**
 * FQresultErrorFieldsAsString()
 *
 * Return all error fields formatted as a single string.
 *
 * Caller must free returned string.
 */
char *
FQresultErrorFieldsAsString(const FBresult *res, char *prefix)
{
	FQExpBufferData buf;
	FBMessageField *mfield;
	char *str;
	bool is_first = true;

	if (!res || res->errFields == NULL)
	{
		str = (char *)malloc(1);
		str[0] = '\0';
		return str;
	}

	initFQExpBuffer(&buf);

	for (mfield = res->errFields; mfield->next != NULL; mfield = mfield->next);

	do {
		if (is_first == true)
			is_first = false;
		else
			appendFQExpBufferChar(&buf, '\n');

		if (prefix != NULL)
			appendFQExpBuffer(&buf, prefix);

		appendFQExpBuffer(&buf, mfield->value);

		mfield = mfield->prev;
	} while ( mfield != NULL);

	str = (char *)malloc(strlen(buf.data) + 1);
	memcpy(str, buf.data, strlen(buf.data) + 1);
	termFQExpBuffer(&buf);

	return str;
}



/**
 * _FQlogLevel()
 *
 * Return a loglevel value as a string
 */
char *_FQlogLevel(short errlevel)
{
	switch(errlevel)
	{
		case INFO:
			return "INFO";
		case NOTICE:
			return "NOTICE";
		case WARNING:
			return "WARNING";
		case ERROR:
			return "ERROR";
		case FATAL:
			return "FATAL";
		case PANIC:
			return "PANIC";
		case DEBUG1:
			return "DEBUG1";
		case DEBUG2:
			return "DEBUG2";
		case DEBUG3:
			return "DEBUG3";
		case DEBUG4:
			return "DEBUG4";
		case DEBUG5:
			return "DEBUG5";
	}

	return "Unknown log level";
}


/**
 * _FQsetResultError()
 *
 * http://ibexpert.net/ibe/index.php?n=Doc.Firebird21ErrorCodes
 * http://www.firebirdsql.org/file/documentation/reference_manuals/reference_material/Firebird-2.1-ErrorCodes.pdf
 * also ibase.h
 */
void
_FQsetResultError(FBconn *conn, FBresult *res)
{
	long *pvector;
	char msg[ERROR_BUFFER_LEN];
	int line = 0;
	FQExpBufferData buf;
	char *error_field = NULL;
	bool skip_line = false;
	int msg_len = 0;

	res->fbSQLCODE = isc_sqlcode(conn->status);

	/* fb_interpret() will modify this pointer */
	pvector = conn->status;

	/*
	 * The first message will be something like "Dynamic SQL Error",
	 * or something like:
	 *   'violation of PRIMARY or UNIQUE KEY constraint "INTEG_276" on table "TBL_SUVYSYHE"'
	 * if no further error fields are available, we'll assume that is
	 * the primary message, and copy it there.
	 *
	 * Applications should not expect FB_DIAG_MESSAGE_TYPE to contain any useful content.
	 */

	fb_interpret(msg, ERROR_BUFFER_LEN, (const ISC_STATUS**) &pvector);
	_FQsaveMessageField(&res, FB_DIAG_MESSAGE_TYPE, msg);

	/*
	 * In theory, the next message returned will always be:
	 *
	 *     SQL error code = -...
	 *
	 * so we can just skip it, as we have the error code anyway.
	 */
	fb_interpret(msg, ERROR_BUFFER_LEN, (const ISC_STATUS**) &pvector);

	/*
	 * Loop through any remaining lines; we assume the next one (line 0) will be the
	 * a message with some useful information, and the one after that (line 1)
	 * will have some detail, usually the name of the problematic object.
	 *
	 * We'll also try and extract line and column numbers; these currently observed
	 * to appear in one of two positions:
	 *
	 *     line 0: "Token unknown - line 1, column 1"
	 *     line 2: "At line 1, column 15"
	 */
	while (fb_interpret(msg, ERROR_BUFFER_LEN, (const ISC_STATUS**) &pvector))
	{
		FQdiagType current_diagType;

		skip_line = false;

		if (line == 0)
		{
			int line, col;
			char *message_part;

			current_diagType = FB_DIAG_MESSAGE_PRIMARY;

			if (sscanf(msg, "%m[^-]- line %d, column %d", &message_part, &line, &col) == 3)
			{
				int msg_len = strlen(message_part);

				if (msg_len >= ERROR_BUFFER_LEN)
					msg_len = ERROR_BUFFER_LEN - 1;

				res->errLine = line;
				res->errCol = col;

				memset(msg, '\0', ERROR_BUFFER_LEN);

				strncpy(msg, message_part, msg_len);
				free(message_part);
			}
		}
		else if (line == 1)
		{
			current_diagType = FB_DIAG_MESSAGE_DETAIL;
		}
		else
		{
			int line, col;

			current_diagType = FB_DIAG_OTHER;

			if (sscanf(msg, "At line %d, column %d", &line, &col) == 2)
			{
				res->errLine = line;
				res->errCol = col;

				skip_line = true;
			}
		}

		if (skip_line == false)
			_FQsaveMessageField(&res, current_diagType, msg);

		line++;
	}

	if (line == 0)
	{
		_FQsaveMessageField(&res, FB_DIAG_MESSAGE_PRIMARY, FQresultErrorField(res, FB_DIAG_MESSAGE_TYPE));
	}

	/*
	 * format the error message into something readable and store it
	 * in both the connection and result structs
	 */

	initFQExpBuffer(&buf);

	/* only add this if it's not the only error message */
	if (line > 0)
	{
		appendFQExpBuffer(&buf,
						  "%s\n", FQresultErrorField(res, FB_DIAG_MESSAGE_TYPE));
	}

	error_field = FQresultErrorField(res, FB_DIAG_MESSAGE_PRIMARY);

	if (error_field != NULL)
	{
		appendFQExpBuffer(&buf,
						  "ERROR: %s\n", error_field);

		error_field = FQresultErrorField(res, FB_DIAG_MESSAGE_DETAIL);
		if (error_field != NULL)
		{
			appendFQExpBuffer(&buf,
							  "DETAIL: %s", error_field);

			if (res->errLine > 0)
			{
				appendFQExpBuffer(&buf,
								  " at line %i, column %i", res->errLine, res->errCol);
			}
		}
	}

	msg_len = strlen(buf.data);

	res->errMsg = (char *)malloc(msg_len + 1);
	memset(res->errMsg, '\0', msg_len + 1);
	strncpy(res->errMsg, buf.data, msg_len);

	if (conn->errMsg != NULL)
		free(conn->errMsg);

	conn->errMsg = (char *)malloc(msg_len + 1);
	memset(conn->errMsg, '\0', msg_len + 1);
	strncpy(conn->errMsg, buf.data, msg_len);

	termFQExpBuffer(&buf);
}


/**
 * _FQsetResultNonFatalError()
 *
 * Handle non-fatal error.
 *
 * This function's behaviour mimics libpq, where a non-fatal error
 * is dumped to STDOUT by default. libpq also allows the client to nominate
 * or handled by a client-specified handler; this functionality is not
 * yet implemented.
 */
void _FQsetResultNonFatalError(const FBconn *conn, FBresult *res, short errlevel, char *msg)
{
	fprintf(stderr, "%s: %s", _FQlogLevel(errlevel), msg);
}


/**
 * _FQsaveMessageField()
 *
 * store one field of an error or notice message
 */
void
_FQsaveMessageField(FBresult **res, FQdiagType code, const char *value, ...)
{
	va_list argp;
	FBMessageField *mfield;

	char buffer[2048];
	int buflen = 0;

	/*
	 * It's possible we're being asked to store a message in a result
	 * which has not yet been initialised; in that case we need to initialise
	 * it ourselces.
	 */
	if (*res == NULL)
	{
		*res = _FQinitResult(false);
	}

	/*
	 * Format the message
	 */
	va_start(argp, value);
	vsnprintf(buffer, sizeof(buffer), value, argp);
	va_end(argp);

	buflen = strlen(buffer);

	/*
	 * Initialize the message field struct
	 */
	mfield = (FBMessageField *)malloc(sizeof(FBMessageField));

	if (mfield == NULL)
		return;

	memset(mfield, '\0', sizeof(FBMessageField));

	mfield->code = code;
	mfield->prev = NULL;
	mfield->value = (char *)malloc(buflen + 1);

	if (mfield->value == NULL)
	{
		free(mfield);
		return;
	}

	memset(mfield->value, '\0', buflen + 1);

	/*
	 * Copy the message. We know the allocated buffer has sufficient space
	 * for the formatted message, so strcpy() is appropriate here.
	 */
	strcpy(mfield->value, buffer);

	/*
	 * Add the message to the errFields linked list.
	 */
	mfield->next = (*res)->errFields;

	if (mfield->next)
		mfield->next->prev = mfield;

	(*res)->errFields = mfield;

	return;
}


/*
 * ==============================
 * Transaction handling functions
 * ==============================
 */


/**
 * FQisActiveTransaction()
 *
 * Indicates whether the provided connection has been marked
 * as being in a user-initiated transaction.
 */
bool
FQisActiveTransaction(FBconn *conn)
{
	if (!conn)
		return false;

	return conn->in_user_transaction;
}


/**
 * FQsetAutocommit()
 *
 * Set connection's autocommit status
 */
void
FQsetAutocommit(FBconn *conn, bool autocommit)
{
	if (conn != NULL)
		conn->autocommit = autocommit;
}


/**
 * FQstartTransaction()
 *
 * Start a transaction using the connection's default transaction handle.
 */
FQtransactionStatusType
FQstartTransaction(FBconn *conn)
{
	if (!conn)
		return TRANS_ERROR;

	return _FQstartTransaction(conn, &conn->trans);
}


/**
 * FQcommitTransaction()
 *
 * Commit a transaction using the connection's default transaction handle.
 */
FQtransactionStatusType
FQcommitTransaction(FBconn *conn)
{
	if (!conn)
		return TRANS_ERROR;

	return _FQcommitTransaction(conn, &conn->trans);
}


/**
 * FQrollbackTransaction()
 *
 * Roll back a tranaction using the connection's default transaction handle.
 */
FQtransactionStatusType
FQrollbackTransaction(FBconn *conn)
{
	if (!conn)
		return TRANS_ERROR;

	return _FQrollbackTransaction(conn, &conn->trans);
}



/**
 * _FQcommitTransaction()
 *
 * Commit the provided transaction handle
 */
static FQtransactionStatusType
_FQcommitTransaction(FBconn *conn, isc_tr_handle *trans)
{
	if (isc_commit_transaction(conn->status, trans))
		return TRANS_ERROR;

	*trans = 0L;

	return TRANS_OK;
}


/**
 * _FQrollbackTransaction()
 *
 * Roll back the provided transaction handle
 */
static FQtransactionStatusType
_FQrollbackTransaction(FBconn *conn, isc_tr_handle *trans)
{
	if (isc_rollback_transaction(conn->status, trans))
		return TRANS_ERROR;

	*trans = 0L;

	return TRANS_OK;
}


/**
 * _FQstartTransaction()
 *
 * Start a transaction using the provided transaction handle
 */
static FQtransactionStatusType
_FQstartTransaction(FBconn *conn, isc_tr_handle *trans)
{
	if (isc_start_transaction(conn->status, trans, 1, &conn->db, 0, NULL))
		return TRANS_ERROR;

	return TRANS_OK;
}


/**
 * _FQformatDatum()
 *
 * Format the provided SQLVAR datum as a FQresTupleAtt
 */
static FQresTupleAtt *
_FQformatDatum(FBconn *conn, FQresTupleAttDesc *att_desc, XSQLVAR *var)
{
	FQresTupleAtt *tuple_att;
	short		   datatype;
	char		  *p = NULL;
	struct tm	   timestamp_utc;
	char		   format_buffer[1024];
	char		   pad_buffer[1024];
	bool		   format_error = false;
	int		   	   s;

	tuple_att = (FQresTupleAtt *)malloc(sizeof(FQresTupleAtt));
	tuple_att->value = NULL;
	tuple_att->len = 0;
	tuple_att->dsplen = 0;
	tuple_att->dsplen_line = 0;
	tuple_att->lines = 1;

	/* If the column is nullable and null, return initialized but empty FQresTupleAtt */
	if ((var->sqltype & 1) && (*var->sqlind < 0))
	{
		tuple_att->has_null = true;
		return tuple_att;
	}

	tuple_att->has_null = false;
	datatype = att_desc->type;

	switch (datatype)
	{
		case SQL_TEXT:

			if (var->sqlsubtype == 1)
			{
				/* column defined as "CHARACTER SET OCTETS" */
				p = _FQformatOctet(var->sqldata, var->sqllen);
			}
			else
			{
				p = (char *)malloc(var->sqllen + 1);

				memcpy(p, var->sqldata, var->sqllen);
				p[var->sqllen] = '\0';
			}
			break;

		case SQL_VARYING:
		{
			VARY2		  *vary2 = (VARY2*)var->sqldata;

			if (var->sqlsubtype == 1)
			{
				/* column defined as "CHARACTER SET OCTETS" */
				p = _FQformatOctet(vary2->vary_string, vary2->vary_length);
			}
			else
			{
				p = (char *)malloc(vary2->vary_length + 1);
				memcpy(p, vary2->vary_string, vary2->vary_length + 1);
				p[vary2->vary_length] = '\0';
			}
		}
			break;
		case SQL_SHORT:
		case SQL_LONG:
		case SQL_INT64:
		{
			ISC_INT64	value = 0;
			short		field_width = 0;
			short		dscale;
			int			buflen;

			switch (datatype)
			{
				case SQL_SHORT:
					value = (ISC_INT64) *(short *) var->sqldata;
					field_width = 6;
					break;
				case SQL_LONG:
					value = (ISC_INT64) *(int *) var->sqldata;
					field_width = 11;
					break;
				case SQL_INT64:
					value = (ISC_INT64) *(ISC_INT64 *) var->sqldata;
					field_width = 21;
					break;
			}

			dscale = var->sqlscale;

			buflen = field_width - 1 + dscale + 1;

			if (dscale < 0)
			{
				ISC_INT64	tens;
				short	i;

				tens = 1;
				for (i = 0; i > dscale; i--)
					tens *= 10;

				if (value >= 0)
				{
					p = (char *)malloc(buflen);
					sprintf(p, "%lld.%0*lld",
							(ISC_INT64) value / tens,
							-dscale,
							(ISC_INT64) value % tens);
				}
				else if ((value / tens) != 0)
				{
					s = snprintf(format_buffer, sizeof(format_buffer), "%lld.%0*lld",
								 (ISC_INT64) (value / tens),
								 -dscale,
								 (ISC_INT64) - (value % tens));
					if (s < 0)
					{
						format_error = true;
					}
					else
					{
						p = (char *)malloc(buflen);
						memset(p, '\0', buflen);
						memcpy(p, format_buffer, buflen - 1);
					}
				}
				else
				{
					s = snprintf(format_buffer, sizeof(format_buffer),
								 "%s.%0*lld",
								 "-0",
								 -dscale,
								 (ISC_INT64) - (value % tens));

					if (s < 0)
					{
						format_error = true;
					}
					else
					{
						p = (char *)malloc(buflen);
						memset(p, '\0', buflen);
						memcpy(p, format_buffer, buflen - 1);
					}
				}
			}
			else if (dscale)
			{
				p = (char *)malloc(field_width + 1);

				sprintf (p, "%lld%0*d",
						 (ISC_INT64) value,
						 dscale, 0);
			}
			else
			{
				p = (char *)malloc(field_width + 1);

				sprintf (p, "%lld",
						 (ISC_INT64) value);
			}
			break;
		}

#if defined SQL_INT128
		/* Firebird 4.0 and later */
		case SQL_INT128:
		{
			__int128 value = (__int128) *(__int128 *) var->sqldata;

			p = (char *)malloc(99 + 1); // XXX
			(void)format_int128((__int128) value, p);
			break;
		}
#endif

		case SQL_FLOAT:
			p = (char *)malloc(FB_FLOAT_LEN + 1);
			sprintf(p, "%g", *(float *) (var->sqldata));
			break;

		case SQL_DOUBLE:
			p = (char *)malloc(FB_DOUBLE_LEN + 1);
			sprintf(p, "%f", *(double *) (var->sqldata));
			break;

		case SQL_TYPE_DATE:
			isc_decode_sql_date((ISC_DATE *)var->sqldata, &timestamp_utc);
			s = snprintf(format_buffer, sizeof(format_buffer),
						 "%04d-%02d-%02d",
						 timestamp_utc.tm_year + 1900,
						 timestamp_utc.tm_mon+1,
						 timestamp_utc.tm_mday);
			if (s < 0)
			{
				format_error = true;
			}
			else
			{
				s = snprintf(pad_buffer, sizeof(pad_buffer),
							 "%*s", FB_DATE_LEN,
							 format_buffer);
				if (s < 0)
				{
					format_error = true;
				}
				else
				{
					int l = strlen(pad_buffer);
					p = (char *)malloc(l + 1);
					memset(p, '\0', l + 1);
					memcpy(p, pad_buffer, l);
				}
			}
			break;

		case SQL_TYPE_TIME:
#ifdef HAVE_TIMEZONE
		case SQL_TIME_TZ:
		case SQL_TIME_TZ_EX:
#endif
			isc_decode_sql_time((ISC_TIME *)var->sqldata, &timestamp_utc);

			if (datatype == SQL_TYPE_TIME)
			{
				s = snprintf(format_buffer, sizeof(format_buffer),
							 "%02d:%02d:%02d.%04d",
							 timestamp_utc.tm_hour,
							 timestamp_utc.tm_min,
							 timestamp_utc.tm_sec,
							 (*((ISC_TIME *)var->sqldata)) % 10000);
			}
#ifdef HAVE_TIMEZONE
			else if (datatype == SQL_TIME_TZ)
			{
				ISC_TIME_TZ *ttz = (ISC_TIME_TZ *)(var->sqldata);
				ISC_USHORT tz = ttz->time_zone;
				char *tz_desc = _FQlookupTimeZone((int)tz);

				s = snprintf(format_buffer, sizeof(format_buffer),
							 "%02d:%02d:%02d.%04d %s",
							 timestamp_utc.tm_hour,
							 timestamp_utc.tm_min,
							 timestamp_utc.tm_sec,
							 (*((ISC_TIME *)var->sqldata)) % 10000,
							 tz_desc);
				free(tz_desc);
			}
			else
			{
				ISC_TIME_TZ_EX *ttz = (ISC_TIME_TZ_EX *)(var->sqldata);
				ISC_USHORT tz = ttz->time_zone;
				ISC_SHORT tz_ext_offset = ttz->ext_offset;

				struct tm*	   timestamp_local;
				time_t time_utc;
				time_t time_local;

				char *tz_desc = NULL;

				time_utc = mktime(&timestamp_utc);
				time_local = time_utc + (tz_ext_offset * 60);
				timestamp_local = localtime(&time_local);

				tz_desc = _FQformatTimeZone((int)tz, (int)tz_ext_offset, conn->time_zone_names);

				s = snprintf(format_buffer, sizeof(format_buffer),
							 "%02d:%02d:%02d.%04d %s",
							 timestamp_local->tm_hour,
							 timestamp_local->tm_min,
							 timestamp_local->tm_sec,
							 (*((ISC_TIME *)var->sqldata)) % 10000,
							 tz_desc);
				free(tz_desc);
			}
#endif

			if (s < 0)
			{
				format_error = true;
			}
			else
			{
				s = snprintf(pad_buffer, sizeof(pad_buffer),
							 "%*s", FB_TIME_LEN,
							 format_buffer);
				if (s < 0)
				{
					format_error = true;
				}
				else
				{
					int l = strlen(pad_buffer);
					p = (char *)malloc(l + 1);
					memset(p, '\0', l + 1);
					memcpy(p, pad_buffer, l);
				}
			}
			break;

		case SQL_TIMESTAMP:
#ifdef HAVE_TIMEZONE
		case SQL_TIMESTAMP_TZ:
		case SQL_TIMESTAMP_TZ_EX:
#endif
			isc_decode_timestamp((ISC_TIMESTAMP *)var->sqldata, &timestamp_utc);
			if (datatype == SQL_TIMESTAMP)
			{
				s = snprintf(format_buffer, sizeof(format_buffer),
							 "%04d-%02d-%02d %02d:%02d:%02d.%04d",
							 timestamp_utc.tm_year + 1900,
							 timestamp_utc.tm_mon+1,
							 timestamp_utc.tm_mday,
							 timestamp_utc.tm_hour,
							 timestamp_utc.tm_min,
							 timestamp_utc.tm_sec,
							 ((ISC_TIMESTAMP *)var->sqldata)->timestamp_time % 10000);
			}
#ifdef HAVE_TIMEZONE
			else if (datatype == SQL_TIMESTAMP_TZ)
			{
				ISC_TIMESTAMP_TZ *tstz = (ISC_TIMESTAMP_TZ *)(var->sqldata);
				ISC_USHORT tz = tstz->time_zone;
				char *tz_desc = _FQlookupTimeZone((int)tz);

				s = snprintf(format_buffer, sizeof(format_buffer),
							 "%04d-%02d-%02d %02d:%02d:%02d.%04d %s",
							 timestamp_utc.tm_year + 1900,
							 timestamp_utc.tm_mon+1,
							 timestamp_utc.tm_mday,
							 timestamp_utc.tm_hour,
							 timestamp_utc.tm_min,
							 timestamp_utc.tm_sec,
							 tstz->utc_timestamp.timestamp_time % 10000,
							 tz_desc);
				free(tz_desc);
			}
			else
			{
				ISC_TIMESTAMP_TZ_EX *tstz = (ISC_TIMESTAMP_TZ_EX *)(var->sqldata);
				ISC_USHORT tz = tstz->time_zone;
				ISC_SHORT tz_ext_offset = tstz->ext_offset;

				struct tm*	   timestamp_local;
				time_t time_utc;
				time_t time_local;

				char *tz_desc = NULL;

				time_utc = mktime(&timestamp_utc);
				time_local = time_utc + (tz_ext_offset * 60);
				timestamp_local = localtime(&time_local);

				tz_desc = _FQformatTimeZone(tz, tz_ext_offset, conn->time_zone_names);

				s = snprintf(format_buffer, sizeof(format_buffer),
							 "%04d-%02d-%02d %02d:%02d:%02d.%04d %s",
							 timestamp_local->tm_year + 1900,
							 timestamp_local->tm_mon+1,
							 timestamp_local->tm_mday,
							 timestamp_local->tm_hour,
							 timestamp_local->tm_min,
							 timestamp_local->tm_sec,
							 tstz->utc_timestamp.timestamp_time % 10000,
							 tz_desc);

				free(tz_desc);
			}
#endif
			if (s < 0)
			{
				format_error = true;
			}
			else
			{
				s = snprintf(pad_buffer, sizeof(pad_buffer),
							 "%*s", FB_TIMESTAMP_LEN,
							 format_buffer);
				if (s < 0)
				{
					format_error = true;
				}
				else
				{
					int l = strlen(pad_buffer);
					p = (char *)malloc(l + 1);
					memset(p, '\0', l + 1);
					memcpy(p, pad_buffer, l);
				}
			}
			break;


        /* BLOBs are tricky...*/
		case SQL_BLOB:
        {
            ISC_QUAD *blob_id = (ISC_QUAD *)var->sqldata;

            /* must be initialised to 0 */
            isc_blob_handle blob_handle = 0;
            char blob_segment[BLOB_SEGMENT_LEN];
            unsigned short actual_seg_len;
            ISC_STATUS blob_status;

            FQExpBufferData blob_output;

            initFQExpBuffer(&blob_output);

            isc_open_blob2(
                conn->status,
                &conn->db,
                &conn->trans,
                &blob_handle, /* set by this function to refer to the BLOB */
                blob_id,      /* Blob ID put into out_sqlda by isc_dsql_fetch() */
                0,            /* BPB length = 0; no filter will be used */
                NULL          /* NULL BPB, since no filter will be used */
                );

            do {
                char *seg;
                blob_status = isc_get_segment(
                    conn->status,
                    &blob_handle,         /* set by isc_open_blob2()*/
                    &actual_seg_len,      /* length of segment read */
                    sizeof(blob_segment), /* length of segment buffer */
                    blob_segment          /* segment buffer */
                    );

                seg = (char *)malloc(sizeof(char) * (actual_seg_len + 1));
                memcpy(seg, blob_segment, actual_seg_len);
                seg[actual_seg_len] = '\0';
                appendFQExpBufferStr(&blob_output, seg);
                free(seg);
            } while (blob_status == 0 || conn->status[1] == isc_segment);

            p = (char *)malloc(strlen(blob_output.data) + 1);
            memcpy(p, blob_output.data, strlen(blob_output.data) + 1);

            /* clean up */
            isc_close_blob(conn->status, &blob_handle);
            termFQExpBuffer(&blob_output);

            break;
        }

#if defined SQL_BOOLEAN
		/* Firebird 3.0 and later */
		case SQL_BOOLEAN:
			p = (char *)malloc(2);
			sprintf(p, "%c", *var->sqldata == FB_TRUE ? 't' : 'f');
			break;
#endif

		/* Special case for RDB$DB_KEY:
		 * copy byte values individually, don't treat as string
		 */
		case SQL_DB_KEY:
		{
			char *p_ptr;
			char *db_key = var->sqldata;
			p = (char *)malloc(var->sqllen + 2);
			p_ptr = p;

			for (; db_key < var->sqldata + var->sqllen; db_key++)
				*p_ptr++ = *db_key;
			break;
		}

		default:
			p = (char *)malloc(64);
			snprintf(p, 64, "Unhandled datatype %i", datatype);
			break;
	}

	/*
	 * We weren't able to format the Datum  due to an error.
	 * It's unlikely this case will be triggered.
	 */
	if (p == NULL)
	{
		p = (char *)malloc(64);
		if (format_error)
			snprintf(p, 64, "Error formatting datatype %i", datatype);
		else
			snprintf(p, 64, "Unknown issue formatting datatype %i", datatype);
	}

	tuple_att->value = p;

    /* Calculate display width */
	/* Special case for RDB$DB_KEY */
	if (datatype == SQL_DB_KEY)
	{
		tuple_att->len = var->sqllen;
		tuple_att->dsplen = FB_DB_KEY_LEN;
	}
	else
	{
	   bool get_dsp_len = false;
		tuple_att->len = strlen(p);

		if (conn->get_dsp_len == true)
		{
			switch(datatype)
			{
				case SQL_TEXT:
				case SQL_VARYING:
					get_dsp_len = true;
					break;

				case SQL_BLOB:
					/* TODO: get blob subtype */
					get_dsp_len = true;
					break;
			}
		}

		if (get_dsp_len == true)
		{
			tuple_att->dsplen = FQdspstrlen(tuple_att->value, FQclientEncodingId(conn));
			tuple_att->dsplen_line = _FQdspstrlen_line(tuple_att, FQclientEncodingId(conn));
		}
		else
		{
			tuple_att->dsplen = tuple_att->len;
			tuple_att->dsplen_line = tuple_att->len;
		}
	}

	return tuple_att;
}


/**
 * _FQformatOctet
 *
 * Display an octet string as hex values in upper case.
 */
static char *
_FQformatOctet(char *data, int len)
{
	int i;
	char *p, *q;


	p = (char *)malloc((len * 2) + 1);
	q = p;

	for (i = 0; i < len; i++)
	{
		if (data[i])
		{

			sprintf(q, "%02X", (unsigned int)(data[i] & 0xFF));
			q += 2;
		}
		else
		{
			sprintf(q, "00");
			q += 2;
		}

	}

	return p;
}


/**
 * FQformatDbKey()
 *
 * Format an RDB$DB_KEY value for output
 */
char *
FQformatDbKey(const FBresult *res,
              int row_number,
              int column_number)
{
	char *value = NULL;

	if (!check_tuple_field_number(res, row_number, column_number))
		return NULL;

	if (FQgetisnull(res, row_number, column_number))
		return NULL;

	value = FQgetvalue(res, row_number, column_number);

	if (value == NULL)
		return NULL;

	return _FQparseDbKey(value);
}


/**
 * _FQparseDbKey()
 *
 * Given an 8-byte sequence representing an RDB$DB_KEY value,
 * return a pointer to a 16-byte hexadecimal representation in ASCII
 */
char *
_FQparseDbKey(const char *db_key)
{
	char *formatted_value;
	unsigned char *t;

	formatted_value = (char *)malloc(FB_DB_KEY_LEN + 1);
	formatted_value[0] = '\0';
	for (t = (unsigned char *) db_key; t < (unsigned char *) db_key + 8; t++)
	{
		char d[3];
		sprintf(d, "%02X", (unsigned char) *t);
		strcat(formatted_value, d);
	}

	return formatted_value;
}


/**
 * _FQdeparseDbKey()
 *
 * Given a 16-byte string representing an RDB$DB_KEY value in
 * ASCII hexadecimal, return 8-byte sequence with raw values.
 */
char *
_FQdeparseDbKey(const char *db_key)
{
	unsigned char *deparsed_value = (unsigned char *)malloc(64);
	unsigned char *outptr;
	const char *inptr;
	char buf[5];

	outptr = deparsed_value;
	for (inptr = db_key; inptr < db_key + FB_DB_KEY_LEN; inptr += 2)
	{
		sprintf(buf, "%c%c", inptr[0], inptr[1]);

		/*
		 * it's highly unlikely that the buffer will contain anything other than
		 * two hexadecimal digits
		 */
		if (sscanf(buf, "%02X", (unsigned int *)outptr) == 1)
			outptr++;
	}

	return (char *)deparsed_value;
}


/**
 * FQclear()
 *
 * Free the storage attached to an FBresult object. Never free() the object
 * itself as that will result in dangling pointers and memory leaks.
 */
void
FQclear(FBresult *result)
{
	int i;

	if (!result)
		return;

	if (result->ntups > 0)
	{
		/* Free header section */
		if (result->header)
		{
			for (i = 0; i < result->ncols; i++)
			{
				if (result->header[i])
				{
					if (result->header[i]->desc != NULL)
						free(result->header[i]->desc);

					if (result->header[i]->alias != NULL)
						free(result->header[i]->alias);

					if (result->header[i]->relname != NULL)
						free(result->header[i]->relname);

					free(result->header[i]);
				}
			}
		}

		free(result->header);

		/* Free any tuples */
		if (result->tuple_first)
		{
			FQresTuple *tuple_ptr = result->tuple_first;
			for (i = 0; i  < result->ntups; i++)
			{
				int j;

				FQresTuple *tuple_next = tuple_ptr->next;

				if (!tuple_next)
					break;

				for (j = 0; j < result->ncols; j++)
				{

					if (tuple_ptr->values[j] != NULL)
					{
						if (tuple_ptr->values[j]->value != NULL)
							free(tuple_ptr->values[j]->value);
						free(tuple_ptr->values[j]);
					}
				}

				free(tuple_ptr->values);
				free(tuple_ptr);

				tuple_ptr = tuple_next;
			}

			if (result->tuples)
				free(result->tuples);
		}
	}

	if (result->errMsg)
		free(result->errMsg);

	if (result->errFields)
	{
		FBMessageField *mfield = result->errFields;

		while (mfield != NULL)
		{
			FBMessageField *mfield_next = mfield->next;
			free(mfield->value);
			free(mfield);
			mfield = mfield_next;
		}
	}

	/*
	 * NOTE: these should be cleared by _FQexecClearResult() anyway
	 * XXX we should call _FQexecClearSQLDA here too
	 */
	if (result->sqlda_in != NULL)
	{
		free(result->sqlda_in);
		result->sqlda_in = NULL;
	}

	if (result->sqlda_out != NULL)
	{
		free(result->sqlda_out);
		result->sqlda_out  = NULL;
	}
	free(result);
}


/**
 * FQexplainStatement()
 *
 * Primitive query plan generator.
 *
 * TODO:
 *  - improve error handling
 *  - strip leading whitespace from returned plan string
 */
char *
FQexplainStatement(FBconn *conn, const char *stmt)
{
	FBresult	  *result;

	char  plan_info[1];
	char  plan_buffer[2048];
	char *plan_out = NULL;
	short plan_length;

	result = _FQinitResult(false);

	if (!conn)
	{
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - invalid connection");

		FQclear(result);
		return NULL;
	}


	if (isc_dsql_allocate_statement(conn->status, &conn->db, &result->stmt_handle) != 0)
	{
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - isc_dsql_allocate_statement");
		_FQsetResultError(conn, result);

		FQclear(result);
		return NULL;
	}

	/* Prepare the statement. */
	if (isc_dsql_prepare(conn->status, &conn->trans, &result->stmt_handle, 0, stmt, SQL_DIALECT_V6, result->sqlda_out))
	{
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - isc_dsql_prepare");
		_FQsetResultError(conn, result);

		FQclear(result);
		return NULL;
	}

	plan_info[0] = isc_info_sql_get_plan;

	if (isc_dsql_sql_info(conn->status, &result->stmt_handle, sizeof(plan_info), plan_info,
						  sizeof(plan_buffer), plan_buffer))
	{
		_FQsaveMessageField(&result, FB_DIAG_DEBUG, "error - isc_dsql_sql_info");
		_FQsetResultError(conn, result);

		FQclear(result);

		return NULL;
	}

	plan_length = (short) isc_vax_integer((char *)plan_buffer + 1, 2);

	if (plan_length)
	{
		plan_out = (char *)malloc(plan_length + 1);
		memset(plan_out, '\0', plan_length + 1);
		memcpy(plan_out, plan_buffer + 3, plan_length);
	}

	FQclear(result);
	return plan_out;
}


/**
 * FQlog()
 *
 * Primitive logging output, mainly for debugging purposes.
 *
 * TODO
 * - optional log destination (specify in FBconn?)
 */
void
FQlog(FBconn *conn, short loglevel, const char *msg, ...)
{
	va_list argp;

	if (!conn)
		return;

	/* Do nothing if loglevel is below the specified threshold */
	if (loglevel < conn->client_min_messages)
		return;

	va_start(argp, msg);
	vprintf(msg, argp);
	va_end(argp);

	puts("");

	fflush(stdout);
}


/**
 * FQmblen()
 *
 * Return the byte length of the provided character
 */
int
FQmblen(const char *s, short encoding_id)
{
	int len = 1;

	switch(encoding_id)
	{
		case FBENC_UTF8:
			len = pg_utf_mblen((const unsigned char *)s);
			break;
	}

	return len;
}


/**
 * FQdsplen()
 *
 * Return the display length of the provided character
 */
int
FQdsplen(const unsigned char *s, short encoding_id)
{
	int len = 1;
	switch(encoding_id)
	{
		case FBENC_UTF8:
			len = pg_utf_dsplen(s);
			break;
	}

	return len;
}


/**
 * FQdspstrlen()
 *
 * Returns the single-character-equivalent display length of the string in
 * the provided encoding
 */
int
FQdspstrlen(const char *s, short encoding_id)
{
	int len = strlen(s);
	int chlen = 0;
	int dsplen = 0;
	int w;

	for (; *s && len > 0; s += chlen)
	{
		chlen = FQmblen(s, encoding_id);

		if (len < (size_t) chlen)
			break;

		w = FQdsplen((const unsigned char *)s, encoding_id);
		dsplen += w;
		len -= chlen;
	}

	return dsplen;
}


/**
 * _FQdspstrlen_line()
 *
 * Returns the single-character-equivalent display length of the longest
 * line from string in the provided encoding
 */
int
_FQdspstrlen_line(FQresTupleAtt *att, short encoding_id)
{
	char *ptr = (char *)att->value;
	int max_len = 0;
	int cur_len = 0;

	while (ptr[0] != '\0')
	{
		if (ptr[0] == '\n'
		||  ptr[0] == '\r'
		|| (ptr[0] == '\n' && ptr[1] == '\r')
		|| (ptr[0] == '\r' && ptr[1] == '\n')
		)
		{
			if (cur_len > max_len)
				max_len = cur_len;

			cur_len = 0;
		}
		else
		{
			cur_len++;
		}
		ptr++;
	}

	return max_len ? max_len : cur_len;
}


static int
_FQgetLogLevelFromName(const char *log_level)
{
	int i = 0;
	struct log_level_entry ll;

	for (;;)
	{
		ll = log_levels[i];

		if (strcmp(log_level, ll.log_level) == 0)
			return ll.log_level_id;

		if (ll.log_level == NULL)
			break;
		i++;
	}

	return 0;
}

static const char*
_FQgetLogLevelName(int log_level_id)
{
	int i = 0;
	struct log_level_entry ll;

	for (;;)
	{
		ll = log_levels[i];

		if (log_level_id == ll.log_level_id)
			return ll.log_level;

		if (ll.log_level == NULL)
			break;
		i++;
	}

	return NULL;
}


#if defined SQL_INT128
#define P10_UINT64 10000000000000000000ULL   /* 19 zeroes */
#define E10_UINT64 19

#define STRINGIZER(x)
#define TO_STRING(x)    STRINGIZER(x)

static int
format_int128(__int128 val, char *dst)
{
	int n;

	if (val > INT64_MAX || val < (0 - INT64_MAX))
    {
		__int128 leading  = val / P10_UINT64;
        int64_t trailing = val % P10_UINT64;
		char buf[99];
		char *bufptr = buf;

        n = format_int128(leading, dst);
        sprintf(buf, "%." TO_STRING(E10_UINT64) PRIi64, trailing);

		/* Don't append a second minus sign */
		if (trailing < 0)
			bufptr++;

		n += sprintf(dst + n, "%s", bufptr);
    }
    else
    {
        int64_t i64 = val;
        n = sprintf(dst, "%" PRIi64, i64);
    }

	return n;
}

static __int128
convert_int128(const char *s)
{
    const char *p = s;
    int neg = 0;
	__int128 val = 0;

    while (isspace(*p))
	{
        p++;
    }

    if ((*p == '-') || (*p == '+'))
	{
        if (*p == '-')
            neg = 1;
        p++;
    }

    while (*p >= '0' && *p <= '9')
	{
        if (neg)
            val = (10 * val) - (*p - '0');
		else
            val = (10 * val) + (*p - '0');

        p++;
    }

    return val;
}

#endif

#ifdef HAVE_TIMEZONE

/**
 * _FQlookupTimeZone()
 *
 * Given the "time_zone" value from one of the following structs:
 *
 *  - ISC_TIME_TZ
 *  - ISC_TIME_TZ_EX
 *  - ISC_TIMESTAMP_TZ
 *  - ISC_TIMESTAMP_TZ_EX
 *
 * returns the time zone as either the time zone name or the time zone offset.
 *
 * Caller should free the returned string.
 */
static char *
_FQlookupTimeZone(int time_zone_id)
{
	int i = 0;
	struct tz_entry tz;
	char *tz_desc;

	/*
	 * "time_zone_id" is a time zone offset.
	 */
	if (time_zone_id <= (FB_TIMEZONE_OFFSET_BASE + FB_TIMEZONE_OFFSET_MAX_MINUTES)
	&&  time_zone_id >= (FB_TIMEZONE_OFFSET_BASE - FB_TIMEZONE_OFFSET_MAX_MINUTES))
	{
		bool offset_positive;
		int offset_raw;
		int offset_hours;
		int offset_minutes;

		if (time_zone_id >= FB_TIMEZONE_OFFSET_BASE)
		{
			offset_positive = true;
			offset_raw = time_zone_id - FB_TIMEZONE_OFFSET_BASE;
		}
		else
		{
			offset_positive = false;
			offset_raw = FB_TIMEZONE_OFFSET_BASE - time_zone_id;
		}

		offset_hours = offset_raw / 60;
		offset_minutes = offset_raw - (offset_hours * 60);

		/*
		 * If we don't do this, the compiler might think that "offset_minutes" could
		 * be in the range [-840, 840]. Which it can't be.
		 *
		 * The alternative would be to write the "offset_minutes" calculation as:
		 *
		 *   offset_raw - ((offset_raw / 60) * 60);
		 */
		if (offset_minutes < 0 || offset_minutes > 59)
			offset_minutes = 0;

		tz_desc = (char *)malloc(FB_TIMEZONE_OFFSET_STRLEN + 1);
		snprintf(tz_desc,
				 FB_TIMEZONE_OFFSET_STRLEN + 1,
				 "%c%02d:%02d",
				 offset_positive ? '+' : '-',
				 offset_hours,
				 offset_minutes);

		return tz_desc;
	}

	/*
	 * Scan list of time zone names for time_zone_id.
	 */
	for (;;)
	{
		tz = timezones[i];

		if (tz.id == time_zone_id)
		{
			int len = strlen(tz.name);
			tz_desc = (char *)malloc(len + 1);
			memset(tz_desc, '\0', len + 1);
			memcpy(tz_desc, tz.name, len);

			return tz_desc;
		}

		/*
		 * A named time zone was not found, so exit the loop and
		 * return an error.
		 */
		if (tz.id == 0)
			break;

		i++;
	}

	/*
	 * The provided identifier is neither an offset nor a known named time zone.
	 */
	tz_desc = (char *)malloc(64);
	snprintf(tz_desc,
			 64,
			 "unexpected time_zone value %i",
			 time_zone_id);
	return tz_desc;
}

/**
 * _FQformatTimeZone()
 *
 * If "time_zone_names" is true, formats the time zone as Firebird does normally
 * (i.e. as the time zone name if it was specified, or the time zone offset),
 * otherwise formats the time zone as an offset.
 *
 * Caller should free the returned string.
 */
static char *
_FQformatTimeZone(int time_zone_id, int tz_ext_offset, bool time_zone_names)
{
	char *tz_desc;
	bool offset_positive;
	int offset_raw;
	int offset_hours;
	int offset_minutes;

	if (time_zone_names == true)
		return _FQlookupTimeZone(time_zone_id);

	if (tz_ext_offset >= 0)
	{
		offset_positive = true;
		offset_raw = tz_ext_offset;
	}
	else
	{
		offset_positive = false;
		offset_raw = abs(tz_ext_offset);
	}

	offset_hours = offset_raw / 60;
	offset_minutes = offset_raw - (offset_hours * 60);

	/*
	 * Sanity checks to keep the compiler happy.
	 */
	if (offset_minutes < 0 || offset_minutes > 59)
		offset_minutes = 0;

	if (offset_hours < -14 || offset_hours > 14)
		offset_hours = 0;

	tz_desc = (char *)malloc(FB_TIMEZONE_OFFSET_STRLEN + 1);
	snprintf(tz_desc,
			 FB_TIMEZONE_OFFSET_STRLEN + 1,
			 "%c%02d:%02d",
			 offset_positive ? '+' : '-',
			 offset_hours,
			 offset_minutes);

	return tz_desc ;
}
#endif
