/*----------------------------------------------------------------------
 *
 * libfq - C API wrapper for Firebird
 *
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

#include "ibase.h"

#include "libfq-int.h"
#include "libfq.h"
#include "libfq-version.h"

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
static FQresult *_FQinitResult(bool init_sqlda_in);
static void _FQexecClearResult(FQresult *result);
static void _FQexecClearSQLDA(FQresult *result, XSQLDA *sqlda);
static void _FQexecFillTuplesArray(FQresult *result);
static void _FQexecInitOutputSQLDA(FBconn *conn, FQresult *result);
static ISC_LONG _FQexecParseStatementType(char *info_buffer);

static FQresult *_FQexec(FBconn *conn, isc_tr_handle *trans, const char *stmt);
static FQresult *_FQexecParams(FBconn *conn,
							   isc_tr_handle *trans,
							   const char *stmt,
							   int nParams,
							   const int *paramTypes,
							   const char * const *paramValues,
							   const int *paramLengths,
							   const int *paramFormats,
							   int resultFormat);

static char *_FQlogLevel(short errlevel);
static void _FQsetResultError(const FBconn *conn, FQresult *res);
static void _FQsetResultNonFatalError(const FBconn *conn, FQresult *res, short errlevel, char *msg);
static void _FQsaveMessageField(FQresult *res, FQdiagType code, const char *value, ...);
static char *_FQdeparseDbKey(const char *db_key);
static char *_FQparseDbKey(const char *db_key);

static void _FQinitClientEncoding(FBconn *conn);
static const char *_FQclientEncoding(const FBconn *conn);


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
 *
 * This list may change in the future.
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

	int i = 0;

	while(keywords[i])
	{
		if (strcmp(keywords[i], "db_path") == 0)
			db_path = values[i];
		else if (strcmp(keywords[i], "user") == 0)
			uname = values[i];
		else if (strcmp(keywords[i], "password") == 0)
			upass = values[i];
		else if (strcmp(keywords[i], "client_encoding") == 0)
			client_encoding = values[i];

		i++;
	}

	/* initialise libfq's connection struct */
	conn = (FBconn *)malloc(sizeof(FBconn));

	conn->db = 0L;
	conn->trans = 0L;
	conn->trans_internal = 0L;
	conn->autocommit = true;
	conn->in_user_transaction = false;
	conn->status =	(ISC_STATUS *) malloc(sizeof(ISC_STATUS) * ISC_STATUS_LENGTH);
	conn->engine_version = NULL;
	conn->client_min_messages = DEBUG1;
	conn->client_encoding = NULL;
	conn->client_encoding_id = -1;	/* indicate the server-parsed value has not yet been retrieved */
	conn->get_dsp_len = false;

	/* Initialise the Firebird parameter buffer */
	conn->dpb_buffer = (char *) malloc((size_t)256);

	dpb = (char *)conn->dpb_buffer;

	*dpb++ = isc_dpb_version1;

	conn->dpb_length = dpb - (char*)conn->dpb_buffer;
	dpb = (char *)conn->dpb_buffer;

	db_path_len = strlen(db_path);

	conn->db_path = malloc(db_path_len + 1);
	strncpy(conn->db_path, db_path, db_path_len);
	conn->db_path[db_path_len] = '\0';

	if (uname != NULL)
	{
		int uname_len = strlen(uname);
		isc_modify_dpb(&dpb, &conn->dpb_length, isc_dpb_user_name, uname, strlen(uname));
		conn->uname = malloc(uname_len + 1);
		strncpy(conn->uname, uname, uname_len);
		conn->uname[uname_len] = '\0';
	}

	if (upass != NULL)
	{
		int upass_len = strlen(upass);
		isc_modify_dpb(&dpb, &conn->dpb_length, isc_dpb_password, upass, strlen(upass));
		conn->upass = malloc(upass_len + 1);
		strncpy(conn->upass, upass, upass_len);
		conn->upass[upass_len] = '\0';
	}

	if (client_encoding == NULL)
	{
		/* reasonably sensible default - but maybe "NONE" better? */
		client_encoding = "UTF8";
	}

	isc_modify_dpb(&dpb, &conn->dpb_length, isc_dpb_lc_ctype, client_encoding, strlen(client_encoding));


	isc_attach_database(
		conn->status,
		db_path_len,
		db_path,
		&conn->db,
		conn->dpb_length,
		dpb
	);

	_FQinitClientEncoding(conn);

	return conn;
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

	free(conn);
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
FQstatus(const FBconn *conn)
{
	if (conn == NULL || conn->db == 0L)
		return CONNECTION_BAD;

	return CONNECTION_OK;
}


/**
 * FQparameterStatus()
 *
 * Returns a current parameter setting from the provided connection object
 */

extern const char *
FQparameterStatus(const FBconn *conn, const char *paramName)
{
	if (conn == NULL)
		return NULL;

	if (strcmp(paramName, "client_encoding") == 0)
		return _FQclientEncoding(conn);

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

		FQresult   *res;

		if (_FQstartTransaction(conn, &conn->trans_internal) == TRANS_ERROR)
			return;

		res = _FQexec(conn, &conn->trans_internal, sql);

		if (FQresultStatus(res) == FBRES_TUPLES_OK && !FQgetisnull(res, 0, 0))
		{
			int major, minor, revision;
			char buf[10];
			int engine_version_len = sizeof(FQgetvalue(res, 0, 0));

			conn->engine_version = malloc(engine_version_len + 1);
			strncpy(conn->engine_version, FQgetvalue(res, 0, 0), engine_version_len);
			conn->engine_version[engine_version_len] = '\0';

			sscanf(conn->engine_version, "%i.%i.%i", &major, &minor, &revision);
			sprintf(buf, "%d%02d%02d", major, minor, revision);
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
		return -1;

	if (conn->client_encoding_id == -1)
		_FQinitClientEncoding(conn);

	/* in case we still couldn't get a valid encoding */
	if (conn->client_encoding_id == -1)
		return -1;

	return conn->client_encoding_id;
}


/**
 * _FQinitClientEncoding()
 *
 */
static void
_FQinitClientEncoding(FBconn *conn)
{
	const char *sql = \
"    SELECT TRIM(rdb$character_set_name) AS client_encoding, " \
"           mon$character_set_id AS client_encoding_id " \
"      FROM mon$attachments " \
"INNER JOIN rdb$character_sets " \
"        ON mon$character_set_id = rdb$character_set_id "\
"     WHERE mon$remote_pid = %i";

	char query[1024];
	FQresult   *res;

	if (_FQstartTransaction(conn, &conn->trans_internal) == TRANS_ERROR)
		return;

	sprintf(query, sql, getpid());

	res = _FQexec(conn, &conn->trans_internal, query);

	if (FQresultStatus(res) == FBRES_TUPLES_OK && !FQgetisnull(res, 0, 0))
	{
		int client_encoding_len = strlen(FQgetvalue(res, 0, 0));

		if (conn->client_encoding != NULL)
			free(conn->client_encoding);

		conn->client_encoding =	 malloc(client_encoding_len + 1);

		strncpy(conn->client_encoding, FQgetvalue(res, 0, 0), client_encoding_len);
		conn->client_encoding[client_encoding_len] = '\0';
		conn->client_encoding_id = (short)atoi(FQgetvalue(res, 0, 1));
	}

	FQclear(res);

	_FQcommitTransaction(conn, &conn->trans_internal);

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
 * Initialise an FQresult object with sensible defaults and
 * preallocate in/out SQLDAs.
 */
static FQresult *
_FQinitResult(bool init_sqlda_in)
{
	FQresult *result;

	result = malloc(sizeof(FQresult));

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

	result->stmt_handle = 0L;
	result->ntups = -1;
	result->ncols = -1;
	result->resultStatus = FBRES_NO_ACTION;
	result->errMsg = NULL;
	result->errFields = NULL;
	result->fbSQLCODE = 0L;

	return result;
}


/**
 * _FQexecClearResult()
 *
 * Free a result object's temporary memory allocations assigned
 * during query execution
 */
static void
_FQexecClearResult(FQresult *result)
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


/**
 * _FQexecClearSQLDA()
 *
 *
 */
static
void _FQexecClearSQLDA(FQresult *result, XSQLDA *sqlda)
{
	XSQLVAR *var;
	short	 i;

	for (i = 0, var = result->sqlda_out->sqlvar; i < result->ncols; var++, i++)
	{
		if (var->sqldata != NULL)
		{
			free(var->sqldata);
		}

		if (var->sqltype & 1 && var->sqlind != NULL)
		{
			/* deallocate NULL status indicator if necessary */
			free(var->sqlind);
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
_FQexecInitOutputSQLDA(FBconn *conn, FQresult *result)
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

			case SQL_TIMESTAMP:
				var->sqldata = (char *)malloc(sizeof(ISC_TIMESTAMP));
				break;
			case SQL_TYPE_DATE:
				var->sqldata = (char *)malloc(sizeof(ISC_DATE));
				break;
			case SQL_TYPE_TIME:
				var->sqldata = (char *)malloc(sizeof(ISC_TIME));
				break;

			case SQL_BLOB:
				var->sqldata = (char *)malloc(sizeof(ISC_QUAD));
				break;

			default:
				sprintf(error_message, "Unhandled sqlda_out type: %i", sqltype);

				_FQsetResultError(conn, result);
				_FQsaveMessageField(result, FB_DIAG_DEBUG, error_message);

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
_FQexecFillTuplesArray(FQresult *result)
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
FQresult *
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
static FQresult *
_FQexec(FBconn *conn, isc_tr_handle *trans, const char *stmt)
{
	FQresult	  *result;

	static char	  stmt_info[] = { isc_info_sql_stmt_type };
	char		  info_buffer[20];
	int			  statement_type;

	int			  num_rows = 0;
	long		  fetch_stat;
	short		  i;

	bool		  temp_trans = false;

	result = _FQinitResult(false);

	/* Allocate a statement. */
	if (isc_dsql_allocate_statement(conn->status, &conn->db, &result->stmt_handle))
	{
		result->resultStatus = FBRES_FATAL_ERROR;
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - isc_dsql_allocate_statement");
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
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - isc_dsql_prepare");

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
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - isc_dsql_sql_info");

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
			temp_trans = false;
			if (*trans == 0L)
			{
				_FQstartTransaction(conn, trans);
				temp_trans = true;
			}

			if (isc_dsql_execute(conn->status, trans,  &result->stmt_handle, SQL_DIALECT_V6, NULL))
			{
				_FQrollbackTransaction(conn, trans);
				_FQsaveMessageField(result, FB_DIAG_DEBUG, "error executing DDL");
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
			_FQsaveMessageField(result, FB_DIAG_DEBUG, "error executing non-SELECT");
			_FQsetResultError(conn, result);

			result->resultStatus = FBRES_FATAL_ERROR;
			_FQexecClearResult(result);
			return result;
		}

		if (conn->autocommit == true	 && conn->in_user_transaction == false)
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
			_FQsaveMessageField(result, FB_DIAG_DEBUG, "isc_dsql_describe");

			result->resultStatus = FBRES_FATAL_ERROR;

			_FQexecClearResult(result);
			return result;
		}

		result->ncols = result->sqlda_out->sqld;
	}

	_FQexecInitOutputSQLDA(conn, result);

	if (isc_dsql_execute(conn->status, trans, &result->stmt_handle, SQL_DIALECT_V6, NULL))
	{
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "isc_dsql_execute error");

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

	while ((fetch_stat = isc_dsql_fetch(conn->status, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_out)) == 0)
	{
		FQresTuple *tuple_next = (FQresTuple *)malloc(sizeof(FQresTuple));

		tuple_next->position = num_rows;
		tuple_next->next = NULL;
		tuple_next->values = malloc(sizeof(FQresTupleAtt) * result->ncols);

		/* store header information */
		if (num_rows == 0)
		{
			for (i = 0; i < result->ncols; i++)
			{
				FQresTupleAttDesc *desc = (FQresTupleAttDesc *)malloc(sizeof(FQresTupleAttDesc));
				XSQLVAR *var = &result->sqlda_out->sqlvar[i];

				desc->desc_len = var->sqlname_length;
				desc->desc = (char *)malloc(desc->desc_len + 1);
				memcpy(desc->desc, var->sqlname, desc->desc_len + 1);
				desc->desc_dsplen = FQdspstrlen(desc->desc, FQclientEncodingId(conn));

				/* Alias is identical to column name - don't duplicate */
				if (var->aliasname_length == var->sqlname_length
				   && strncmp(var->aliasname, var->sqlname, var->aliasname_length ) == 0)
				{
					desc->alias_len = 0;
					desc->alias = NULL;
				}
				else
				{
					desc->alias_len = var->aliasname_length;
					desc->alias = (char *)malloc(desc->alias_len + 1);
					memcpy(desc->alias, var->aliasname, desc->alias_len + 1);
					desc->alias_dsplen = FQdspstrlen(desc->alias, FQclientEncodingId(conn));
				}
				desc->att_max_len = 0;

				/* Firebird returns RDB$DB_KEY as "DB_KEY" - set the pseudo-datatype*/
				if (strncmp(desc->desc, "DB_KEY", 6) == 0 && strlen(desc->desc) == 6)
					desc->type = SQL_DB_KEY;
				else
					desc->type = var->sqltype & ~1;

				desc->has_null = false;
				result->header[i] = desc;
			}
		}

		for (i = 0; i < result->ncols; i++)
		{
			XSQLVAR *var = (XSQLVAR *)&result->sqlda_out->sqlvar[i];
			FQresTupleAtt *tuple_att = _FQformatDatum(conn, result->header[i], var);

			if (tuple_att->value == NULL)
				result->header[i]->has_null = true;
			else
				if (tuple_att->dsplen > result->header[i]->att_max_len)
					result->header[i]->att_max_len = tuple_att->dsplen;

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


		num_rows++;
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
FQresult *
FQexecParams(FBconn *conn,
			 const char *stmt,
			 int nParams,
			 const int *paramTypes,
			 const char * const *paramValues,
			 const int *paramLengths,
			 const int *paramFormats,
			 int resultFormat
	)
{
	if (!conn)
		return NULL;

	return _FQexecParams(conn,
						 &conn->trans,
						 stmt,
						 nParams,
						 paramTypes,
						 paramValues,
						 paramLengths,
						 paramFormats,
						 resultFormat
		);
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
FQresult *
_FQexecParams(FBconn *conn,
			  isc_tr_handle *trans,
			  const char *stmt,
			  int nParams,
			  const int *paramTypes,
			  const char * const *paramValues,
			  const int *paramLengths,
			  const int *paramFormats,
			  int resultFormat
	)
{
	FQresult	 *result;
	XSQLVAR		 *var;
	bool		  temp_trans = false;
	int			  i, num_rows = 0;

	long		  fetch_stat;
	char		  info_buffer[20];
	static char	  stmt_info[] = { isc_info_sql_stmt_type };
	int			  statement_type;

	char		  error_message[1024];

	result = _FQinitResult(true);

	if (*trans == 0L)
	{
		_FQstartTransaction(conn, trans);
		temp_trans = true;
	}

	/* Allocate a statement. */
	if (isc_dsql_alloc_statement2(conn->status, &conn->db, &result->stmt_handle))
	{
		result->resultStatus = FBRES_FATAL_ERROR;
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - isc_dsql_allocate_statement");
		_FQsetResultError(conn, result);

		_FQexecClearResult(result);
		return result;
	}

	/* Prepare the statement. */
	if (isc_dsql_prepare(conn->status, trans, &result->stmt_handle, 0, stmt, SQL_DIALECT_V6, NULL))
	{
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - isc_dsql_prepare");

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
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - isc_dsql_sql_info");

		_FQsetResultError(conn, result);

		_FQrollbackTransaction(conn, trans);
		result->resultStatus = FBRES_FATAL_ERROR;

		_FQexecClearResult(result);
		return result;
	}

	statement_type = _FQexecParseStatementType((char *) info_buffer);

	FQlog(conn, DEBUG1, "statement_type: %i", statement_type);

	switch(statement_type)
	{
		case isc_info_sql_stmt_insert:
		case isc_info_sql_stmt_update:
		case isc_info_sql_stmt_delete:
		case isc_info_sql_stmt_select:
		case isc_info_sql_stmt_exec_procedure:
			/* INSERT ... RETURNING ... */
			break;

		default:
			_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - stmt type is not DML");

			_FQsetResultError(conn, result);

			_FQrollbackTransaction(conn, trans);
			result->resultStatus = FBRES_FATAL_ERROR;

			_FQexecClearResult(result);
			return result;
	}

	if (isc_dsql_describe_bind(conn->status, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_in))
	{
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - isc_dsql_describe_bind");
		_FQsetResultError(conn, result);
		result->resultStatus = FBRES_FATAL_ERROR;

		_FQrollbackTransaction(conn, trans);

		_FQexecClearResult(result);
		return result;
	}

	if (*trans == 0L)
	{
		FQlog(conn, DEBUG1, "execP: starting trans...");
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

	for (i = 0, var = result->sqlda_in->sqlvar; i < result->sqlda_in->sqld; i++, var++)
	{
		int dtype = (var->sqltype & ~1); /* drop flag bit for now */
		struct tm tm;

		int len = 0;

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

				case SQL_TIMESTAMP:
					size = sizeof(ISC_TIMESTAMP);
					break;

				case SQL_TYPE_DATE:
					size = sizeof(ISC_DATE);
					break;

				case SQL_TYPE_TIME:
					size = sizeof(ISC_TIME);
					break;

				default:
					sprintf(error_message, "Unhandled sqlda_in type: %i", dtype);

					_FQsetResultError(conn, result);
					_FQsaveMessageField(result, FB_DIAG_DEBUG, error_message);

					result->resultStatus = FBRES_FATAL_ERROR;

					_FQexecClearResult(result);
			}

			if (size >= 0)
			{
				var->sqldata = (char *)malloc(size);
				var->sqllen = size;
			}
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
					if ( paramFormats != NULL && paramFormats[i] == -1)
					{
						unsigned char *sqlptr;
						unsigned char *srcptr;
						int ix = 0;

						srcptr = (unsigned char *)_FQdeparseDbKey(paramValues[i]);
						FQlog(conn, DEBUG1, "srcptr %s",  _FQparseDbKey((char *)srcptr));
						len = 8;
						var->sqllen = len;
						var->sqldata = (char *)malloc(len);
						sqlptr = (unsigned char *)var->sqldata ;
						for (ix = 0; ix < len; ix++)
						{
							*sqlptr++ = *srcptr++;
						}
					}
					else {
						len = strlen(paramValues[i]);
						var->sqldata = (char *)malloc(sizeof(char) * len);
						var->sqllen = len;
						memcpy(var->sqldata, paramValues[i], len);
					}

					break;

				case SQL_TIMESTAMP:
				case SQL_TYPE_DATE:
				case SQL_TYPE_TIME:
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

				default:
					sprintf(error_message, "Unhandled sqlda_in type: %i", dtype);

					_FQsetResultError(conn, result);
					_FQsaveMessageField(result, FB_DIAG_DEBUG, error_message);

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
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "isc_dsql_describe");

		result->resultStatus = FBRES_FATAL_ERROR;
		_FQexecClearResult(result);
		return result;
	}

	/* Expand output sqlda to required number of columns */
	result->ncols = result->sqlda_out->sqld;

	/* No output expected */
	if (!result->ncols)
	{
		if (isc_dsql_execute(conn->status, trans, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_in))
		{
			FQlog(conn, DEBUG1, "isc_dsql_execute(): error");

			_FQsaveMessageField(result, FB_DIAG_DEBUG, "isc_dsql_execute() error");

			_FQsetResultError(conn, result);
			result->resultStatus = FBRES_FATAL_ERROR;

			FQresultDumpErrorFields(conn, result);

			/* if autocommit, and no explicit transaction set, rollback */
			if (conn->autocommit == true && conn->in_user_transaction == false)
			{
				_FQrollbackTransaction(conn, trans);
			}

			_FQexecClearResult(result);
			return result;
		}

		FQlog(conn, DEBUG1, "finished non-select");
		result->resultStatus = FBRES_COMMAND_OK;
		if (conn->autocommit == true && conn->in_user_transaction == false)
		{
			FQlog(conn, DEBUG1, "committing...");
			_FQcommitTransaction(conn, trans);
		}

		_FQexecClearResult(result);
		return result;
	}

	if (result->sqlda_out->sqln < result->ncols) {
		free(result->sqlda_out);
		result->sqlda_out = (XSQLDA *) malloc(XSQLDA_LENGTH (result->ncols));
		memset(result->sqlda_out, '\0', XSQLDA_LENGTH (result->ncols));

		result->sqlda_out->version = SQLDA_VERSION1;
		result->sqlda_out->sqln = result->ncols;

		result->ncols = result->sqlda_out->sqld;
		isc_dsql_describe(conn->status, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_out);
	}

	_FQexecInitOutputSQLDA(conn, result);

	if (isc_dsql_execute2(conn->status, trans, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_in, NULL))
	{
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "isc_dsql_execute2() error");

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

	/* XXX TODO: verify if this is needed */
	if (isc_dsql_set_cursor_name(conn->status, &result->stmt_handle, "dyn_cursor", 0))
	{
		_FQsetResultError(conn, result);
		_FQsaveMessageField(result, FB_DIAG_DEBUG, error_message);

		result->resultStatus = FBRES_FATAL_ERROR;

		_FQexecClearResult(result);
		return result;
	}

	while ((fetch_stat = isc_dsql_fetch(conn->status, &result->stmt_handle, SQL_DIALECT_V6, result->sqlda_out)) == 0)
	{
		FQresTuple *tuple_next = (FQresTuple *)malloc(sizeof(FQresTuple));

		tuple_next->position = num_rows;
		tuple_next->next = NULL;
		tuple_next->values = malloc(sizeof(FQresTupleAtt) * result->ncols);

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
				desc->att_max_len = 0;

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

			if (tuple_att->value == NULL)
				result->header[i]->has_null = true;
			else
				if (tuple_att->dsplen > result->header[i]->att_max_len)
					result->header[i]->att_max_len = tuple_att->dsplen;

			tuple_next->values[i] = tuple_att;
			//result->tuple_last->values[i]  = tuple_att;
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

		num_rows++;
	}


	/*
	 * HACK: INSERT/UPDATE/DELETE ... RETURNING ... sometimes results in a
	 * "request synchronization error" - ignoring this doesn't seem to
	 * cause any problems. Potentially this is related to issues with cursor
	 * usage.
	 *
	 * See maybe: http://support.codegear.com/article/35153
	 */
	if (fetch_stat != 100L && fetch_stat != isc_req_sync)
	{
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - isc_dsql_fetch reported %lu", fetch_stat);

		_FQsetResultError(conn, result);

		_FQrollbackTransaction(conn, trans);
		result->resultStatus = FBRES_FATAL_ERROR;

		_FQexecClearResult(result);
		return result;
	}

	result->ntups = num_rows;
	_FQexecClearResult(result);

	if (isc_dsql_free_statement(conn->status, &result->stmt_handle, DSQL_drop))
	{
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - isc_dsql_free_statement");

		_FQsetResultError(conn, result);

		_FQrollbackTransaction(conn, trans);
		result->resultStatus = FBRES_FATAL_ERROR;

		return result;
	}

	/* add an array for offset-based access */
	_FQexecFillTuplesArray(result);

	result->resultStatus = FBRES_TUPLES_OK;

	/* if autocommit, and no explicit transaction set, commit */
	if (conn->autocommit == true && conn->in_user_transaction == false)
		_FQcommitTransaction(conn, trans);

	return result;
}


/**
 * FQexecTransaction()
 *
 * Convenience function to execute a query using the internal
 * transaction handle.
 */
FQresult *
FQexecTransaction(FBconn *conn, const char *stmt)
{
	FQresult	  *result = NULL;

	if (!conn)
	{
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - invalid connection object");
		_FQsetResultError(conn, result);

		return NULL;
	}

	if (_FQstartTransaction(conn, &conn->trans_internal) == TRANS_ERROR)
	{
		/* XXX todo: set error, return result */
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "transaction error");
		isc_print_status(conn->status);
		return NULL;
	}

	result = _FQexec(conn, &conn->trans_internal, stmt);

	if (FQresultStatus(result) == FBRES_FATAL_ERROR)
	{
		/* XXX todo: set error */
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "query execution error");
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
			_FQsaveMessageField(result, FB_DIAG_DEBUG, "transaction commit error");
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
FQresultStatus(const FQresult *res)
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
 * FQntuples()
 *
 * Returns the number of tuples (rows) in the provided result.
 * Defaults to -1 if no query has been executed.
 */
int
FQntuples(const FQresult *res)
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
FQnfields(const FQresult *res)
{
	if (!res)
		return -1;

	return res->ncols;
}


/**
 * FQgetvalue()
 *
 * Returns a single field of an FQresult.
 *
 * Row and column numbers start at 0.
 *
 * NOTE: this function will return NULL if invalid row/column parameters
 *   are provided, as well as when the tuple value is actually NULL.
 *   To determine if a tuple value is null, use FQgetisnull().
 *
 */
char *
FQgetvalue(const FQresult *res,
           int row_number,
           int column_number)
{
	if (!res)
		return NULL;

	if (row_number >= res->ntups)
		return NULL;

	if (column_number >= res->ncols)
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
FQgetisnull(const FQresult *res,
            int row_number,
            int column_number)
{
	if (!res)
		return 0;

	if (res->tuples[row_number]->values[column_number]->has_null == true)
		return 1;

	return 0;
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
FQfhasNull(const FQresult *res, int column_number)
{
	if (!res)
		return false;

	if (column_number >= res->ncols)
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
FQfmaxwidth(const FQresult *res, int column_number)
{
	int max_width;

	if (!res || !res->header)
		return 0;

	if (column_number >= res->ncols)
		return 0;

	if (res->header[column_number]->alias_len)
		max_width = res->header[column_number]->att_max_len > res->header[column_number]->alias_dsplen
			? res->header[column_number]->att_max_len
			: res->header[column_number]->alias_dsplen;
	else
		max_width = res->header[column_number]->att_max_len > res->header[column_number]->desc_dsplen
			? res->header[column_number]->att_max_len
			: res->header[column_number]->desc_dsplen;

	return max_width;
}


/**
 * FQfname()
 *
 * Provides the name (or alias, if set) of a particular field (column).
 */
char *
FQfname(const FQresult *res, int column_number)
{
	if (!res)
		return NULL;

	if (column_number >= res->ncols)
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
FQgetlength(const FQresult *res,
            int row_number,
            int column_number)
{
	if (!res)
		return -1;

	if (row_number >= res->ntups)
		return -1;

	if (column_number >= res->ncols)
		return -1;

	return res->tuples[row_number]->values[column_number]->len;
}


/**
 * FQgetdsplen()
 *
 * Returns the display length in single characters of the specified FQresult
 * field.
 *
 * Row and column numbers start at 0.
 */
int
FQgetdsplen(const FQresult *res,
            int row_number,
            int column_number)
{
	if (!res)
		return -1;

	if (row_number >= res->ntups)
		return -1;

	if (column_number >= res->ncols)
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
FQfformat(const FQresult *res, int column_number)
{
	if (!res)
		return -1;

	if (column_number >= res->ncols)
		return -1;

	switch(FQftype(res, column_number))
	{
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
FQftype(const FQresult *res, int column_number)
{
	if (!res)
		return SQL_INVALID_TYPE;

	if (column_number >= res->ncols)
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

	/* XXX todo */
	return "";
}


/**
 * FQresultErrorMessage()
 *
 * Returns the error message associated with the result, or an empty string.
 */
char *
FQresultErrorMessage(const FQresult *result)
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
FQresultErrorField(const FQresult *res, FQdiagType fieldcode)
{
	FBMessageField *mfield;

	if (!res || !res->errFields)
		return NULL;

	for (mfield = res->errFields; mfield != NULL; mfield = mfield->next)
	{
		if (mfield->code == fieldcode)
			return mfield->value;
	}

	return NULL;
}


/**
 * FQresultErrorFieldsAsString()
 *
 * Return all error fields formatted as a single string
 */
char *
FQresultErrorFieldsAsString(const FQresult *res, char *prefix)
{
	FQExpBufferData buf;
	FBMessageField *mfield;
	char *str;
	bool is_first = true;

	if (!res || res->errFields == NULL)
		return "";

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
	} while( mfield != NULL);

	// XXX is this kosher? Check callers free string!
	str = (char *)malloc(strlen(buf.data) + 1);
	memcpy(str, buf.data, strlen(buf.data) + 1);
	termFQExpBuffer(&buf);
	return str;
}


/**
 * FQresultDumpErrorFields()
 *
 * Temporary function to dump the error fields in reverse
 * order, until we can find a way of assigning appropriate diagnostic
 * codes to each field
 */
void
FQresultDumpErrorFields(FBconn *conn, const FQresult *res)
{
	FBMessageField *mfield;

	if (!res || res->errFields == NULL)
		return;

	/* scan to last field */
	for (mfield = res->errFields; mfield->next != NULL; mfield = mfield->next);

	for (; mfield != NULL;  mfield = mfield->prev)
		FQlog(conn, DEBUG1, "* %s", mfield->value);
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
_FQsetResultError(const FBconn *conn, FQresult *res)
{
	long *pvector;
	char msg[ERROR_BUFFER_LEN];

	res->fbSQLCODE = isc_sqlcode(conn->status);

	pvector = conn->status;

	fb_interpret(msg, ERROR_BUFFER_LEN, (const ISC_STATUS**) &pvector);

	res->errMsg = (char *)malloc(strlen(msg) + 1);
	memcpy(res->errMsg, msg, strlen(msg) + 1);

	while(fb_interpret(msg, ERROR_BUFFER_LEN, (const ISC_STATUS**) &pvector))
	{
		/* XXX todo: get appropriate FB_DIAG_* code */
		_FQsaveMessageField(res, FB_DIAG_OTHER, msg);
	}
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
void _FQsetResultNonFatalError(const FBconn *conn, FQresult *res, short errlevel, char *msg)
{
	fprintf(stderr, "%s: %s", _FQlogLevel(errlevel), msg);
}


/**
 * _FQsaveMessageField()
 *
 * store one field of an error or notice message
 */
void
_FQsaveMessageField(FQresult *res, FQdiagType code, const char *value, ...)
{
	va_list argp;
	FBMessageField *mfield;

	char buffer[2048];

	va_start(argp, value);
	vsnprintf(buffer, 2048, value, argp);
	va_end(argp);

	mfield = (FBMessageField *)
		malloc(sizeof(FBMessageField));

	if (!mfield)
		return;

	mfield->code = code;
	mfield->prev = NULL;
	mfield->value = (char *)malloc(strlen(buffer) + 1);

	if (!mfield->value)
	{
		free(mfield);
		return;
	}

	memcpy(mfield->value, buffer, strlen(buffer) + 1);
	mfield->next = res->errFields;
	if (mfield->next)
		mfield->next->prev = mfield;
	res->errFields = mfield;
}


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
	short		datatype;
	char		*p;
	VARY2		*vary2;
	struct tm	times;
	char		date_buffer[FB_TIMESTAMP_LEN + 1];

	tuple_att = (FQresTupleAtt *)malloc(sizeof(FQresTupleAtt));
	tuple_att->value = NULL;
	tuple_att->len = 0;
	tuple_att->dsplen = 0;
	tuple_att->has_null = false;

	datatype = att_desc->type;

	/* If the column is nullable and null, return initialized but empty FQresTupleAtt */
	if ((var->sqltype & 1) && (*var->sqlind < 0))
	{
		tuple_att->has_null = true;
		return tuple_att;
	}

	switch (datatype)
	{
		case SQL_TEXT:
			p = (char *)malloc(var->sqllen + 1);

			memcpy(p, var->sqldata, var->sqllen);
			p[var->sqllen] = '\0';
			break;

		case SQL_VARYING:
			vary2 = (VARY2*)var->sqldata;
			p = (char *)malloc(vary2->vary_length + 1);
			vary2->vary_string[vary2->vary_length] = '\0';
			memcpy(p, vary2->vary_string, strlen(vary2->vary_string) + 1);
			break;

		case SQL_SHORT:
		case SQL_LONG:
		case SQL_INT64:
		{
			ISC_INT64	value = 0;
			short		field_width = 0;
			short		dscale;

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
			if (dscale < 0)
			{
				ISC_INT64	tens;
				short	i;

				tens = 1;
				for (i = 0; i > dscale; i--)
					tens *= 10;

				if (value >= 0)
				{
					p = (char *)malloc(field_width - 1 + dscale + 1);
					sprintf (p, "%lld.%0*lld",
							 (ISC_INT64) value / tens,
							 -dscale,
							 (ISC_INT64) value % tens
						);
				}
				else if ((value / tens) != 0)
				{
					p = (char *)malloc(field_width - 1 + dscale + 1);

					sprintf (p, "%lld.%0*lld",
							 (ISC_INT64) (value / tens),
							 -dscale,
							 (ISC_INT64) - (value % tens)
						);
				}
				else
				{
					p = (char *)malloc(field_width - 1 + dscale + 1);

					sprintf (p, "%s.%0*lld",
							 "-0",
							 -dscale,
							 (ISC_INT64) - (value % tens)
						);
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
		}
		break;

		case SQL_FLOAT:
			p = (char *)malloc(FB_FLOAT_LEN + 1);
			sprintf(p, "%g", *(float *) (var->sqldata));
			break;

		case SQL_DOUBLE:
			p = (char *)malloc(FB_DOUBLE_LEN + 1);
			sprintf(p, "%f", *(double *) (var->sqldata));
			break;

		case SQL_TIMESTAMP:
			p = (char *)malloc(FB_TIMESTAMP_LEN + 1);
			isc_decode_timestamp((ISC_TIMESTAMP *)var->sqldata, &times);
			sprintf(date_buffer, "%04d-%02d-%02d %02d:%02d:%02d.%04d",
					times.tm_year + 1900,
					times.tm_mon+1,
					times.tm_mday,
					times.tm_hour,
					times.tm_min,
					times.tm_sec,
					((ISC_TIMESTAMP *)var->sqldata)->timestamp_time % 10000);
			sprintf(p, "%*s", FB_TIMESTAMP_LEN, date_buffer);
			break;

		case SQL_TYPE_DATE:
			p = (char *)malloc(FB_DATE_LEN + 1);
			isc_decode_sql_date((ISC_DATE *)var->sqldata, &times);
			sprintf(date_buffer, "%04d-%02d-%02d",
					times.tm_year + 1900,
					times.tm_mon+1,
					times.tm_mday);
			sprintf(p, "%*s", FB_DATE_LEN, date_buffer);
			break;

		case SQL_TYPE_TIME:
			p = (char *)malloc(FB_TIME_LEN + 1);
			isc_decode_sql_time((ISC_TIME *)var->sqldata, &times);
			sprintf(date_buffer, "%02d:%02d:%02d.%04d",
					times.tm_hour,
					times.tm_min,
					times.tm_sec,
					(*((ISC_TIME *)var->sqldata)) % 10000);
			sprintf(p, "%*s", FB_TIME_LEN, date_buffer);
			break;

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
			sprintf(p, "Unhandled datatype %i", datatype);
	}

	tuple_att->value = p;

	/* Special case for RDB$DB_KEY */
	if (datatype == SQL_DB_KEY)
	{
		tuple_att->len = var->sqllen;
		tuple_att->dsplen = FB_DB_KEY_LEN;
	}
	else
	{
		tuple_att->len = strlen(p);

		if (conn->get_dsp_len == true)
			tuple_att->dsplen = FQdspstrlen(tuple_att->value, FQclientEncodingId(conn));
		else
			tuple_att->dsplen = tuple_att->len;
	}

	return tuple_att;
}


/**
 * FQformatDbKey()
 *
 * Format an RDB$DB_KEY value for output
 */
char *
FQformatDbKey(const FQresult *res,
              int row_number,
              int column_number)
{
	char *value = NULL;

	if (!res)
		return NULL;

	if (row_number >= res->ntups)
		return NULL;

	if (column_number >= res->ncols)
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

		sscanf(buf, "%02X", (unsigned int *)outptr);

		outptr++;
	}

	return (char *)deparsed_value;
}


/**
 * FQclear()
 *
 * Free the storage attached to an FQresult object. Never free() the object
 * itself as that will result in dangling pointers and memory leaks.
 */
void
FQclear(FQresult *result)
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

				if (!tuple_ptr)
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
		FBMessageField *mfield;
		for (mfield = result->errFields; mfield != NULL; mfield = mfield->next)
		{
			free(mfield->value);
			free(mfield);
		}
	}

	if (result->sqlda_in != NULL)
		free(result->sqlda_in);

	if (result->sqlda_out != NULL)
		free(result->sqlda_out);

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
	FQresult	  *result;

	char  plan_info[1];
	char  plan_buffer[2048];
	char *plan_out = NULL;
	short plan_length;

	result = _FQinitResult(false);

	if (!conn)
	{
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - invalid connection");
		_FQsetResultError(conn, result);

		FQclear(result);
		return NULL;
	}


	if (isc_dsql_allocate_statement(conn->status, &conn->db, &result->stmt_handle) != 0)
	{
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - isc_dsql_allocate_statement");
		_FQsetResultError(conn, result);

		FQclear(result);
		return NULL;
	}

	/* Prepare the statement. */
	if (isc_dsql_prepare(conn->status, &conn->trans, &result->stmt_handle, 0, stmt, SQL_DIALECT_V6, result->sqlda_out))
	{
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - isc_dsql_prepare");
		_FQsetResultError(conn, result);

		FQclear(result);
		return NULL;
	}

	plan_info[0] = isc_info_sql_get_plan;

	if (isc_dsql_sql_info(conn->status, &result->stmt_handle, sizeof(plan_info), plan_info,
						  sizeof(plan_buffer), plan_buffer))
	{
		_FQsaveMessageField(result, FB_DIAG_DEBUG, "error - isc_dsql_sql_info");
		_FQsetResultError(conn, result);

		return NULL;
	}

	plan_length = (short) isc_vax_integer((char *)plan_buffer + 1, 2) + 1;

	if (plan_length)
	{
		plan_out = (char *)malloc(plan_length + 1);
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
			len = pg_utf_mblen(s);
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

		w = FQdsplen(s, encoding_id);
		dsplen += w;
		len -= chlen;
	}

	return dsplen;
}


/**
 * _FQclientEncoding()
 *
 */
static const char *
_FQclientEncoding(const FBconn *conn)
{
	return conn->client_encoding;
}


