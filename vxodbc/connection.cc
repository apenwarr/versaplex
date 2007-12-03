/*
 * Description:	This module contains routines related to
 *		connecting to and disconnecting from the Postgres DBMS.
 */
/* Multibyte support	Eiji Tokuya 2001-03-15 */

#include <libpq-fe.h>
#include "connection.h"

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#ifndef	WIN32
#include <errno.h>
#endif				/* WIN32 */

#include "environ.h"
#include "socket.h"
#include "statement.h"
#include "qresult.h"
#include "dlg_specific.h"

#include "multibyte.h"

#include "pgapifunc.h"

#include <wvdbusconn.h>
#include <wvistreamlist.h>

#define STMT_INCREMENT 16	/* how many statement holders to allocate
				 * at a time */

#define PRN_NULLCHECK

extern GLOBAL_VALUES globals;


RETCODE SQL_API PGAPI_AllocConnect(HENV henv, HDBC FAR * phdbc)
{
    EnvironmentClass *env = (EnvironmentClass *) henv;
    ConnectionClass *conn;
    CSTR func = "PGAPI_AllocConnect";

    mylog("%s: entering...\n", func);

    conn = CC_Constructor();
    mylog("**** %s: henv = %p, conn = %p\n", func, henv, conn);

    if (!conn)
    {
	env->errormsg =
	    "Couldn't allocate memory for Connection object.";
	env->errornumber = ENV_ALLOC_ERROR;
	*phdbc = SQL_NULL_HDBC;
	EN_log_error(func, "", env);
	return SQL_ERROR;
    }

    if (!EN_add_connection(env, conn))
    {
	env->errormsg = "Maximum number of connections exceeded.";
	env->errornumber = ENV_ALLOC_ERROR;
	CC_Destructor(conn);
	*phdbc = SQL_NULL_HDBC;
	EN_log_error(func, "", env);
	return SQL_ERROR;
    }

    if (phdbc)
	*phdbc = (HDBC) conn;

    return SQL_SUCCESS;
}


RETCODE SQL_API
PGAPI_Connect(HDBC hdbc,
	      const SQLCHAR FAR * szDSN,
	      SQLSMALLINT cbDSN,
	      const SQLCHAR FAR * szUID,
	      SQLSMALLINT cbUID,
	      const SQLCHAR FAR * szAuthStr, SQLSMALLINT cbAuthStr)
{
    ConnectionClass *conn = (ConnectionClass *) hdbc;
    ConnInfo *ci;
    CSTR func = "PGAPI_Connect";
    RETCODE ret = SQL_SUCCESS;
    char fchar;

    mylog("%s: entering..cbDSN=%hi.\n", func, cbDSN);

    if (!conn)
    {
	CC_log_error(func, "", NULL);
	return SQL_INVALID_HANDLE;
    }

    ci = &conn->connInfo;

    make_string(szDSN, cbDSN, ci->dsn, sizeof(ci->dsn));

    /* get the values for the DSN from the registry */
    memcpy(&ci->drivers, &globals, sizeof(globals));
    getDSNinfo(ci, CONN_OVERWRITE);
    logs_on_off(1, true, true);
    /* initialize pg_version from connInfo.protocol    */
    CC_initialize_pg_version(conn);

    conn->dbus = new WvDBusConn(ci->dbus_moniker);

    /*
     * override values from DSN info with UID and authStr(pwd) This only
     * occurs if the values are actually there.
     */
    // VX_CLEANUP: These may be unused.
    fchar = ci->username[0];	/* save the first byte */
    make_string(szUID, cbUID, ci->username, sizeof(ci->username));
    if ('\0' == ci->username[0])	/* an empty string is specified */
	ci->username[0] = fchar;	/* restore the original username */
    fchar = ci->password[0];
    make_string(szAuthStr, cbAuthStr, ci->password,
		sizeof(ci->password));
    if ('\0' == ci->password[0])	/* an empty string is specified */
	ci->password[0] = fchar;	/* restore the original password */

    /* fill in any defaults */
    getDSNdefaults(ci);

    qlog("conn = %p, %s(DSN='%s', UID='%s', PWD='%s')\n", conn, func,
	 ci->dsn, ci->username, ci->password ? "xxxxx" : "");

    mylog("%s: returning..%d.\n", func, ret);

    return ret;
}


RETCODE SQL_API
PGAPI_BrowseConnect(HDBC hdbc,
		    const SQLCHAR FAR * szConnStrIn,
		    SQLSMALLINT cbConnStrIn,
		    SQLCHAR FAR * szConnStrOut,
		    SQLSMALLINT cbConnStrOutMax,
		    SQLSMALLINT FAR * pcbConnStrOut)
{
    CSTR func = "PGAPI_BrowseConnect";
    ConnectionClass *conn = (ConnectionClass *) hdbc;

    mylog("%s: entering...\n", func);

    CC_set_error(conn, CONN_NOT_IMPLEMENTED_ERROR,
		 "Function not implemented", func);
    return SQL_ERROR;
}


/* Drop any hstmts open on hdbc and disconnect from database */
RETCODE SQL_API PGAPI_Disconnect(HDBC hdbc)
{
    ConnectionClass *conn = (ConnectionClass *) hdbc;
    CSTR func = "PGAPI_Disconnect";


    mylog("%s: entering...\n", func);

    if (!conn)
    {
	CC_log_error(func, "", NULL);
	return SQL_INVALID_HANDLE;
    }

    qlog("conn=%p, %s\n", conn, func);

    if (conn->status == CONN_EXECUTING)
    {
	CC_set_error(conn, CONN_IN_USE,
		     "A transaction is currently being executed", func);
	return SQL_ERROR;
    }

    logs_on_off(-1, true, true);
    mylog("%s: about to CC_cleanup\n", func);

    /* Close the connection and free statements */
    CC_cleanup(conn);

    mylog("%s: done CC_cleanup\n", func);
    mylog("%s: returning...\n", func);

    return SQL_SUCCESS;
}


RETCODE SQL_API PGAPI_FreeConnect(HDBC hdbc)
{
    ConnectionClass *conn = (ConnectionClass *) hdbc;
    CSTR func = "PGAPI_FreeConnect";

    mylog("%s: entering...\n", func);
    mylog("**** in %s: hdbc=%p\n", func, hdbc);

    if (!conn)
    {
	CC_log_error(func, "", NULL);
	return SQL_INVALID_HANDLE;
    }

    /* Remove the connection from the environment */
    if (!EN_remove_connection(conn->henv, conn))
    {
	CC_set_error(conn, CONN_IN_USE,
		     "A transaction is currently being executed", func);
	return SQL_ERROR;
    }

    CC_Destructor(conn);

    mylog("%s: returning...\n", func);

    return SQL_SUCCESS;
}


void CC_conninfo_init(ConnInfo * conninfo)
{
    memset(conninfo, 0, sizeof(ConnInfo));
    conninfo->disallow_premature = -1;
    conninfo->allow_keyset = -1;
    conninfo->lf_conversion = -1;
    conninfo->true_is_minus1 = -1;
    conninfo->int8_as = -101;
    conninfo->bytea_as_longvarbinary = -1;
    conninfo->use_server_side_prepare = -1;
    conninfo->lower_case_identifier = -1;
    conninfo->rollback_on_error = -1;
    conninfo->force_abbrev_connstr = -1;
    conninfo->bde_environment = -1;
    conninfo->fake_mss = -1;
    conninfo->cvt_null_date_string = -1;
#ifdef	_HANDLE_ENLIST_IN_DTC_
    conninfo->xa_opt = -1;
    conninfo->autocommit_normal = 0;
#endif				/* _HANDLE_ENLIST_IN_DTC_ */
    memcpy(&(conninfo->drivers), &globals, sizeof(globals));
}

#ifdef	WIN32
extern int platformId;
#endif				/* WIN32 */

/*
 *		IMPLEMENTATION CONNECTION CLASS
 */

static void reset_current_schema(ConnectionClass * self)
{
    if (self->current_schema)
    {
	free(self->current_schema);
	self->current_schema = NULL;
    }
}

ConnectionClass *CC_Constructor()
{
    extern int exepgm;
    ConnectionClass *rv, *retrv = NULL;

    rv = (ConnectionClass *) calloc(sizeof(ConnectionClass), 1);

    if (rv != NULL)
    {
	// rv->henv = NULL;             /* not yet associated with an environment */

	// rv->__error_message = NULL;
	// rv->__error_number = 0;
	// rv->sqlstate[0] = '\0';
	// rv->errormsg_created = FALSE;

	rv->status = CONN_NOT_CONNECTED;
	rv->transact_status = CONN_IN_AUTOCOMMIT;	/* autocommit by default */
	
	CC_conninfo_init(&(rv->connInfo));
	rv->sock = SOCK_Constructor(rv);
	if (!rv->sock)
	    goto cleanup;

	rv->stmts =
	    (StatementClass **) malloc(sizeof(StatementClass *) *
				       STMT_INCREMENT);
	if (!rv->stmts)
	    goto cleanup;
	memset(rv->stmts, 0, sizeof(StatementClass *) * STMT_INCREMENT);

	rv->num_stmts = STMT_INCREMENT;
	rv->descs =
	    (DescriptorClass **) malloc(sizeof(DescriptorClass *) *
					STMT_INCREMENT);
	if (!rv->descs)
	    goto cleanup;
	memset(rv->descs, 0,
	       sizeof(DescriptorClass *) * STMT_INCREMENT);

	rv->num_descs = STMT_INCREMENT;

	// rv->ncursors = 0;
	// rv->ntables = 0;
	// rv->col_info = NULL;

	// rv->translation_option = 0;
	// rv->translation_handle = NULL;
	// rv->DataSourceToDriver = NULL;
	// rv->DriverToDataSource = NULL;
	rv->driver_version = ODBCVER;
#ifdef	WIN32
	if (VER_PLATFORM_WIN32_WINDOWS == platformId
	    && rv->driver_version > 0x0300)
	    rv->driver_version = 0x0300;
#endif				/* WIN32 */
	// memset(rv->pg_version, 0, sizeof(rv->pg_version));
	// rv->pg_version_number = .0;
	// rv->pg_version_major = 0;
	// rv->pg_version_minor = 0;
	// rv->ms_jet = 0;
	if (1 == exepgm)
	    rv->ms_jet = 1;
	// rv->unicode = 0;
	// rv->result_uncommitted = 0;
	// rv->schema_support = 0;
	rv->isolation = SQL_TXN_READ_COMMITTED;
	// rv->original_client_encoding = NULL;
	// rv->current_client_encoding = NULL;
	// rv->server_encoding = NULL;
	// rv->current_schema = NULL;
	// rv->num_discardp = 0;
	// rv->discardp = NULL;
	rv->mb_maxbyte_per_char = 1;
	rv->max_identifier_length = -1;
	rv->escape_in_literal = ESCAPE_IN_LITERAL;

	/* Initialize statement options to defaults */
	/* Statements under this conn will inherit these options */

	InitializeStatementOptions(&rv->stmtOptions);
	InitializeARDFields(&rv->ardOptions);
	InitializeAPDFields(&rv->apdOptions);
#ifdef	_HANDLE_ENLIST_IN_DTC_
	// rv->asdum = NULL;
#endif				/* _HANDLE_ENLIST_IN_DTC_ */
	INIT_CONNLOCK(rv);
	INIT_CONN_CS(rv);
	retrv = rv;
    }

  cleanup:
    if (rv && !retrv)
	CC_Destructor(rv);
    return retrv;
}


char CC_Destructor(ConnectionClass * self)
{
    mylog("enter CC_Destructor, self=%p\n", self);

    if (self->status == CONN_EXECUTING)
	return 0;

    CC_cleanup(self);		/* cleanup socket and statements */

    mylog("after CC_Cleanup\n");
    
    if (self->dbus)
	delete self->dbus;

    /* Free up statement holders */
    if (self->stmts)
    {
	free(self->stmts);
	self->stmts = NULL;
    }
    if (self->descs)
    {
	free(self->descs);
	self->descs = NULL;
    }
    mylog("after free statement holders\n");

    NULL_THE_NAME(self->schemaIns);
    NULL_THE_NAME(self->tableIns);
    if (self->__error_message)
	free(self->__error_message);
    DELETE_CONN_CS(self);
    DELETE_CONNLOCK(self);
    free(self);

    mylog("exit CC_Destructor\n");

    return 1;
}


/*	Return how many cursors are opened on this connection */
int CC_cursor_count(ConnectionClass * self)
{
    StatementClass *stmt;
    int i, count = 0;
    QResultClass *res;

    mylog("CC_cursor_count: self=%p, num_stmts=%d\n", self,
	  self->num_stmts);

    CONNLOCK_ACQUIRE(self);
    for (i = 0; i < self->num_stmts; i++)
    {
	stmt = self->stmts[i];
	if (stmt && (res = SC_get_Result(stmt)) && QR_get_cursor(res))
	    count++;
    }
    CONNLOCK_RELEASE(self);

    mylog("CC_cursor_count: returning %d\n", count);

    return count;
}


void CC_clear_error(ConnectionClass * self)
{
    if (!self)
	return;
    CONNLOCK_ACQUIRE(self);
    self->__error_number = 0;
    if (self->__error_message)
    {
	free(self->__error_message);
	self->__error_message = NULL;
    }
    self->sqlstate[0] = '\0';
    self->errormsg_created = FALSE;
    CONNLOCK_RELEASE(self);
}

// VX_CLEANUP: This is a fairly sensible API request, but CC_send_query isn't
// about to implement it sensibly.  Most people who call this either we don't
// care about, or we should reimplement.
/*
 *	Used to begin a transaction.
 */
char CC_begin(ConnectionClass * self)
{
    char ret = TRUE;
    if (!CC_is_in_trans(self))
    {
	QResultClass *res = CC_send_query(self, "BEGIN", NULL, 0, NULL);
	mylog("CC_begin:  sending BEGIN!\n");

	ret = QR_command_maybe_successful(res);
	QR_Destructor(res);
    }

    return ret;
}

/*
 *	Used to commit a transaction.
 *	We are almost always in the middle of a transaction.
 */
char CC_commit(ConnectionClass * self)
{
    char ret = TRUE;
    if (CC_is_in_trans(self))
    {
	QResultClass *res =
	    CC_send_query(self, "COMMIT", NULL, 0, NULL);
	mylog("CC_commit:  sending COMMIT!\n");
	ret = QR_command_maybe_successful(res);
	QR_Destructor(res);
    }

    return ret;
}

/*
 *	Used to cancel a transaction.
 *	We are almost always in the middle of a transaction.
 */
char CC_abort(ConnectionClass * self)
{
    char ret = TRUE;
    if (CC_is_in_trans(self))
    {
	QResultClass *res =
	    CC_send_query(self, "ROLLBACK", NULL, 0, NULL);
	mylog("CC_abort:  sending ABORT!\n");
	ret = QR_command_maybe_successful(res);
	QR_Destructor(res);
    }

    return ret;
}


/* This is called by SQLDisconnect also */
char CC_cleanup(ConnectionClass * self)
{
    int i;
    StatementClass *stmt;
    DescriptorClass *desc;

    if (self->status == CONN_EXECUTING)
	return FALSE;

    mylog("in CC_Cleanup, self=%p\n", self);

    /* Cancel an ongoing transaction */
    /* We are always in the middle of a transaction, */
    /* even if we are in auto commit. */
    if (self->sock)
    {
	CC_abort(self);

	mylog("after CC_abort\n");

	/* This actually closes the connection to the dbase */
	SOCK_Destructor(self->sock);
	self->sock = NULL;
    }

    mylog("after SOCK destructor\n");

    /* Free all the stmts on this connection */
    for (i = 0; i < self->num_stmts; i++)
    {
	stmt = self->stmts[i];
	if (stmt)
	{
	    stmt->hdbc = NULL;	/* prevent any more dbase interactions */

	    SC_Destructor(stmt);

	    self->stmts[i] = NULL;
	}
    }
    /* Free all the descs on this connection */
    for (i = 0; i < self->num_descs; i++)
    {
	desc = self->descs[i];
	if (desc)
	{
	    DC_get_conn(desc) = NULL;	/* prevent any more dbase interactions */
	    DC_Destructor(desc);
	    free(desc);
	    self->descs[i] = NULL;
	}
    }

    /* Check for translation dll */
#ifdef WIN32
    if (self->translation_handle)
    {
	FreeLibrary(self->translation_handle);
	self->translation_handle = NULL;
    }
#endif

    self->status = CONN_NOT_CONNECTED;
    self->transact_status = CONN_IN_AUTOCOMMIT;
    CC_conninfo_init(&(self->connInfo));
    if (self->original_client_encoding)
    {
	free(self->original_client_encoding);
	self->original_client_encoding = NULL;
    }
    if (self->current_client_encoding)
    {
	free(self->current_client_encoding);
	self->current_client_encoding = NULL;
    }
    if (self->server_encoding)
    {
	free(self->server_encoding);
	self->server_encoding = NULL;
    }
    reset_current_schema(self);
    /* Free cached table info */
    if (self->col_info)
    {
	for (i = 0; i < self->ntables; i++)
	{
	    if (self->col_info[i]->result)	/* Free the SQLColumns result structure */
		QR_Destructor(self->col_info[i]->result);

	    NULL_THE_NAME(self->col_info[i]->schema_name);
	    NULL_THE_NAME(self->col_info[i]->table_name);
	    free(self->col_info[i]);
	}
	free(self->col_info);
	self->col_info = NULL;
    }
    self->ntables = 0;
    self->coli_allocated = 0;
    if (self->num_discardp > 0 && self->discardp)
    {
	for (i = 0; i < self->num_discardp; i++)
	    free(self->discardp[i]);
	self->num_discardp = 0;
    }
    if (self->discardp)
    {
	free(self->discardp);
	self->discardp = NULL;
    }

    mylog("exit CC_Cleanup\n");
    return TRUE;
}


int CC_set_translation(ConnectionClass * self)
{

#ifdef WIN32
    CSTR func = "CC_set_translation";

    if (self->translation_handle != NULL)
    {
	FreeLibrary(self->translation_handle);
	self->translation_handle = NULL;
    }

    if (self->connInfo.translation_dll[0] == 0)
	return TRUE;

    self->translation_option = atoi(self->connInfo.translation_option);
    self->translation_handle =
	LoadLibrary(self->connInfo.translation_dll);

    if (self->translation_handle == NULL)
    {
	CC_set_error(self, CONN_UNABLE_TO_LOAD_DLL,
		     "Could not load the translation DLL.", func);
	return FALSE;
    }

    self->DataSourceToDriver
	=
	(DataSourceToDriverProc) GetProcAddress(self->
						translation_handle,
						"SQLDataSourceToDriver");

    self->DriverToDataSource
	=
	(DriverToDataSourceProc) GetProcAddress(self->
						translation_handle,
						"SQLDriverToDataSource");

    if (self->DataSourceToDriver == NULL
	|| self->DriverToDataSource == NULL)
    {
	CC_set_error(self, CONN_UNABLE_TO_LOAD_DLL,
		     "Could not find translation DLL functions.", func);
	return FALSE;
    }
#endif
    return TRUE;
}


// VX_CLEANUP: This goes as soon as we get rid of QR_next_tuple and 
// SendSyncAndReceive
int
handle_error_message(ConnectionClass * self, char *msgbuf,
		     size_t buflen, char *sqlstate, const char *comment,
		     QResultClass * res)
{
    BOOL new_format = FALSE, msg_truncated = FALSE, truncated, hasmsg =
	FALSE;
    SocketClass *sock = self->sock;
    char msgbuffer[ERROR_MSG_LENGTH];
    UDWORD abort_opt;

    inolog("handle_error_message prptocol=%s\n",
	   self->connInfo.protocol);
    if (PROTOCOL_74(&(self->connInfo)))
	new_format = TRUE;

    inolog("new_format=%d\n", new_format);
    if (new_format)
    {
	size_t msgl;

	msgbuf[0] = '\0';
	for (;;)
	{
	    truncated =
		SOCK_get_string(sock, msgbuffer, sizeof(msgbuffer));
	    if (!msgbuffer[0])
		break;

	    mylog("%s: 'E' - %s\n", comment, msgbuffer);
	    qlog("ERROR from backend during %s: '%s'\n", comment,
		 msgbuffer);
	    msgl = strlen(msgbuffer + 1);
	    switch (msgbuffer[0])
	    {
	    case 'S':
		if (buflen > 0)
		{
		    strncat(msgbuf, msgbuffer + 1, buflen);
		    buflen -= msgl;
		}
		if (buflen > 0)
		{
		    strncat(msgbuf, ": ", buflen);
		    buflen -= 2;
		}
		break;
	    case 'M':
	    case 'D':
		if (buflen > 0)
		{
		    if (hasmsg)
		    {
			strcat(msgbuf, "\n");
			buflen--;
		    }
		    if (buflen > 0)
		    {
			strncat(msgbuf, msgbuffer + 1, buflen);
			buflen -= msgl;
		    }
		}
		if (truncated)
		    msg_truncated = truncated;
		hasmsg = TRUE;
		break;
	    case 'C':
		if (sqlstate)
		    strncpy(sqlstate, msgbuffer + 1, 8);
		break;
	    }
	    if (buflen < 0)
		buflen = 0;
	    while (truncated)
		truncated =
		    SOCK_get_string(sock, msgbuffer, sizeof(msgbuffer));
	}
    } else
    {
	msg_truncated = SOCK_get_string(sock, msgbuf, (Int4) buflen);

	/* Remove a newline */
	if (msgbuf[0] != '\0'
	    && msgbuf[(int) strlen(msgbuf) - 1] == '\n')
	    msgbuf[(int) strlen(msgbuf) - 1] = '\0';

	mylog("%s: 'E' - %s\n", comment, msgbuf);
	qlog("ERROR from backend during %s: '%s'\n", comment, msgbuf);
	for (truncated = msg_truncated; truncated;)
	    truncated =
		SOCK_get_string(sock, msgbuffer, sizeof(msgbuffer));
    }
    abort_opt = 0;
    if (!strncmp(msgbuffer, "FATAL", 5))
    {
	CC_set_errornumber(self, CONNECTION_SERVER_REPORTED_ERROR);
	abort_opt = CONN_DEAD;
    } else
    {
	CC_set_errornumber(self, CONNECTION_SERVER_REPORTED_WARNING);
	if (CC_is_in_trans(self))
	    CC_set_in_error_trans(self);
    }
    if (0 != abort_opt
#ifdef	_LEGACY_MODE_
	|| TRUE
#endif				/* _LEGACY_NODE_ */
	)
	CC_on_abort(self, abort_opt);
    if (res)
    {
	QR_set_rstatus(res, PORES_FATAL_ERROR);
	QR_set_message(res, msgbuf);
	QR_set_aborted(res, TRUE);
    }

    return msg_truncated;
}

// VX_CLEANUP: This goes as soon as we get rid of QR_next_tuple and
// SendSyncAndReceive
int
handle_notice_message(ConnectionClass * self, char *msgbuf,
		      size_t buflen, char *sqlstate,
		      const char *comment, QResultClass * res)
{
    BOOL new_format = FALSE, msg_truncated = FALSE, truncated, hasmsg =
	FALSE;
    SocketClass *sock = self->sock;
    char msgbuffer[ERROR_MSG_LENGTH];

    if (PROTOCOL_74(&(self->connInfo)))
	new_format = TRUE;

    if (new_format)
    {
	size_t msgl;

	msgbuf[0] = '\0';
	for (;;)
	{
	    truncated =
		SOCK_get_string(sock, msgbuffer, sizeof(msgbuffer));
	    if (!msgbuffer[0])
		break;

	    mylog("%s: 'N' - %s\n", comment, msgbuffer);
	    qlog("NOTICE from backend during %s: '%s'\n", comment,
		 msgbuffer);
	    msgl = strlen(msgbuffer + 1);
	    switch (msgbuffer[0])
	    {
	    case 'S':
		if (buflen > 0)
		{
		    strncat(msgbuf, msgbuffer + 1, buflen);
		    buflen -= msgl;
		}
		if (buflen > 0)
		{
		    strncat(msgbuf, ": ", buflen);
		    buflen -= 2;
		}
		break;
	    case 'M':
	    case 'D':
		if (buflen > 0)
		{
		    if (hasmsg)
		    {
			strcat(msgbuf, "\n");
			buflen--;
		    }
		    if (buflen > 0)
		    {
			strncat(msgbuf, msgbuffer + 1, buflen);
			buflen -= msgl;
		    }
		} else
		    msg_truncated = TRUE;
		if (truncated)
		    msg_truncated = truncated;
		hasmsg = TRUE;
		break;
	    case 'C':
		if (sqlstate && !sqlstate[0]
		    && strcmp(msgbuffer + 1, "00000"))
		    strncpy(sqlstate, msgbuffer + 1, 8);
		break;
	    }
	    if (buflen < 0)
		msg_truncated = TRUE;
	    while (truncated)
		truncated =
		    SOCK_get_string(sock, msgbuffer, sizeof(msgbuffer));
	}
    } else
    {
	msg_truncated = SOCK_get_string(sock, msgbuf, (Int4) buflen);

	/* Remove a newline */
	if (msgbuf[0] != '\0' && msgbuf[strlen(msgbuf) - 1] == '\n')
	    msgbuf[strlen(msgbuf) - 1] = '\0';

	mylog("%s: 'N' - %s\n", comment, msgbuf);
	qlog("NOTICE from backend during %s: '%s'\n", comment, msgbuf);
	for (truncated = msg_truncated; truncated;)
	    truncated =
		SOCK_get_string(sock, msgbuffer, sizeof(msgbuffer));
    }
    if (res)
    {
	if (QR_command_successful(res))
	    QR_set_rstatus(res, PORES_NONFATAL_ERROR);
	QR_set_notice(res, msgbuf);	/* will dup this string */
    }

    return msg_truncated;
}

// VX_CLEANUP: Likely junk
void getParameterValues(ConnectionClass * conn)
{
    SocketClass *sock = conn->sock;
    /* ERROR_MSG_LENGTH is suffcient */
    char msgbuffer[ERROR_MSG_LENGTH + 1];

    SOCK_get_string(sock, msgbuffer, sizeof(msgbuffer));
    inolog("parameter name=%s\n", msgbuffer);
    if (stricmp(msgbuffer, "server_encoding") == 0)
    {
	SOCK_get_string(sock, msgbuffer, sizeof(msgbuffer));
	if (conn->server_encoding)
	    free(conn->server_encoding);
	conn->server_encoding = strdup(msgbuffer);
    } else if (stricmp(msgbuffer, "client_encoding") == 0)
    {
	SOCK_get_string(sock, msgbuffer, sizeof(msgbuffer));
	if (conn->current_client_encoding)
	    free(conn->current_client_encoding);
	conn->current_client_encoding = strdup(msgbuffer);
    } else if (stricmp(msgbuffer, "server_version") == 0)
    {
	char szVersion[32];
	int major, minor;

	SOCK_get_string(sock, msgbuffer, sizeof(msgbuffer));
	strncpy(conn->pg_version, msgbuffer, sizeof(conn->pg_version));
	strcpy(szVersion, "0.0");
	if (sscanf(conn->pg_version, "%d.%d", &major, &minor) >= 2)
	{
	    snprintf(szVersion, sizeof(szVersion), "%d.%d", major,
		     minor);
	    conn->pg_version_major = major;
	    conn->pg_version_minor = minor;
	}
	conn->pg_version_number = (float) atof(szVersion);
	if (PG_VERSION_GE(conn, 7.3))
	    conn->schema_support = 1;

	mylog("Got the PostgreSQL version string: '%s'\n",
	      conn->pg_version);
	mylog("Extracted PostgreSQL version number: '%1.1f'\n",
	      conn->pg_version_number);
	qlog("    [ PostgreSQL version string = '%s' ]\n",
	     conn->pg_version);
	qlog("    [ PostgreSQL version number = '%1.1f' ]\n",
	     conn->pg_version_number);
    } else
	SOCK_get_string(sock, msgbuffer, sizeof(msgbuffer));

    inolog("parameter value=%s\n", msgbuffer);
}

// VX_CLEANUP: Likely junk
static int protocol3_opts_array(ConnectionClass * self,
				const char *opts[][2], BOOL libpqopt,
				int dim_opts)
{
    ConnInfo *ci = &(self->connInfo);
    const char *enc = NULL;
    int cnt;

    cnt = 0;
    if (libpqopt && ci->server[0])
    {
	opts[cnt][0] = "host";
	opts[cnt++][1] = ci->server;
    }
    if (libpqopt && ci->port[0])
    {
	opts[cnt][0] = "port";
	opts[cnt++][1] = ci->port;
    }
    if (ci->database[0])
    {
	if (libpqopt)
	{
	    opts[cnt][0] = "dbname";
	    opts[cnt++][1] = ci->database;
	} else
	{
	    opts[cnt][0] = "database";
	    opts[cnt++][1] = ci->database;
	}
    }
    if (ci->username[0])
    {
	opts[cnt][0] = "user";
	opts[cnt++][1] = ci->username;
    }
    if (libpqopt)
    {
	if (ci->sslmode[0])
	{
	    opts[cnt][0] = "sslmode";
	    opts[cnt++][1] = ci->sslmode;
	}
	if (ci->password[0])
	{
	    opts[cnt][0] = "password";
	    opts[cnt++][1] = ci->password;
	}
    } else
    {
	/* DateStyle */
	opts[cnt][0] = "DateStyle";
	opts[cnt++][1] = "ISO";
	/* extra_float_digits */
	opts[cnt][0] = "extra_float_digits";
	opts[cnt++][1] = "2";
	/* geqo */
	opts[cnt][0] = "geqo";
	opts[cnt++][1] = "on";
	/* client_encoding */
	enc =
	    get_environment_encoding(self,
				     self->original_client_encoding,
				     NULL, TRUE);
	if (enc)
	{
	    mylog("startup client_encoding=%s\n", enc);
	    opts[cnt][0] = "client_encoding";
	    opts[cnt++][1] = enc;
	}
    }

    return cnt;
}

// VX_CLEANUP: Junk
static int protocol3_packet_build(ConnectionClass * self)
{
    CSTR func = "protocol3_packet_build";
    SocketClass *sock = self->sock;
    size_t slen;
    char *packet, *ppacket;
    ProtocolVersion pversion;
    const char *opts[20][2];
    int cnt, i;

    cnt =
	protocol3_opts_array(self, opts, FALSE,
			     sizeof(opts) / sizeof(opts[0]));

    slen = sizeof(ProtocolVersion);
    for (i = 0; i < cnt; i++)
    {
	slen += (strlen(opts[i][0]) + 1);
	slen += (strlen(opts[i][1]) + 1);
    }
    slen++;

    if (packet = (char *)malloc(slen), !packet)
    {
	CC_set_error(self, CONNECTION_SERVER_NOT_REACHED,
		     "Could not allocate a startup packet", func);
	return 0;
    }

    mylog("sizeof startup packet = %d\n", slen);

    sock->pversion = PG_PROTOCOL_LATEST;
    /* Send length of Authentication Block */
    SOCK_put_int(sock, (Int4) (slen + 4), 4);

    ppacket = packet;
    pversion = (ProtocolVersion) htonl(sock->pversion);
    memcpy(ppacket, &pversion, sizeof(pversion));
    ppacket += sizeof(pversion);
    for (i = 0; i < cnt; i++)
    {
	strcpy(ppacket, opts[i][0]);
	ppacket += (strlen(opts[i][0]) + 1);
	strcpy(ppacket, opts[i][1]);
	ppacket += (strlen(opts[i][1]) + 1);
    }
    *ppacket = '\0';

    SOCK_put_n_char(sock, packet, (Int4) slen);
    SOCK_flush_output(sock);
    free(packet);

    return 1;
}

static char CC_initial_log(ConnectionClass * self, const char *func)
{
    const ConnInfo *ci = &self->connInfo;
    char vermsg[128];

    snprintf(vermsg, sizeof(vermsg), "Driver Version='%s,%s'"
#ifdef	WIN32
	     " linking"
#ifdef	_MT
#ifdef	_DLL
	     " dynamic"
#else
	     " static"
#endif				/* _DLL */
	     " Multithread"
#else
	     " Singlethread"
#endif				/* _MT */
#ifdef	NOT_USED
#ifdef	_DEBUG
	     " Debug"
#else
	     " Release"
#endif				/* DEBUG */
#endif				/* NOT_USED */
	     " library"
#endif				/* WIN32 */
	     "\n", VXODBCDRIVERVERSION, PG_BUILD_VERSION);
    qlog(vermsg);
    mylog(vermsg);

    if (self->original_client_encoding)
	self->ccsc = pg_CS_code((const UCHAR *)self->original_client_encoding);
    if (self->status != CONN_NOT_CONNECTED)
    {
	CC_set_error(self, CONN_OPENDB_ERROR, "Already connected.",
		     func);
	return 0;
    }

    mylog
	("%s: DSN = '%s', server = '%s', port = '%s', database = '%s', username = '%s', password='%s'\n",
	 func, ci->dsn, ci->server, ci->port, ci->database,
	 ci->username, ci->password ? "xxxxx" : "");

    if (ci->port[0] == '\0' ||
#ifdef	WIN32
	ci->server[0] == '\0' ||
#endif				/* WIN32 */
	ci->database[0] == '\0')
    {
	CC_set_error(self, CONN_INIREAD_ERROR,
		     "Missing server name, port, or database name in call to CC_connect.",
		     func);
	return 0;
    }

    return 1;
}


char CC_add_statement(ConnectionClass * self, StatementClass * stmt)
{
    int i;
    char ret = TRUE;

    mylog("CC_add_statement: self=%p, stmt=%p\n", self, stmt);

    CONNLOCK_ACQUIRE(self);
    for (i = 0; i < self->num_stmts; i++)
    {
	if (!self->stmts[i])
	{
	    stmt->hdbc = self;
	    self->stmts[i] = stmt;
	    break;
	}
    }

    if (i >= self->num_stmts)	/* no more room -- allocate more memory */
    {
	self->stmts =
	    (StatementClass **) realloc(self->stmts,
					sizeof(StatementClass *) *
					(STMT_INCREMENT +
					 self->num_stmts));
	if (!self->stmts)
	    ret = FALSE;
	else
	{
	    memset(&self->stmts[self->num_stmts], 0,
		   sizeof(StatementClass *) * STMT_INCREMENT);

	    stmt->hdbc = self;
	    self->stmts[self->num_stmts] = stmt;

	    self->num_stmts += STMT_INCREMENT;
	}
    }
    CONNLOCK_RELEASE(self);

    return TRUE;
}

static void CC_set_error_statements(ConnectionClass * self)
{
    int i;

    mylog("CC_error_statements: self=%p\n", self);

    for (i = 0; i < self->num_stmts; i++)
    {
	if (NULL != self->stmts[i])
	    SC_ref_CC_error(self->stmts[i]);
    }
}


char CC_remove_statement(ConnectionClass * self, StatementClass * stmt)
{
    int i;
    char ret = FALSE;

    CONNLOCK_ACQUIRE(self);
    for (i = 0; i < self->num_stmts; i++)
    {
	if (self->stmts[i] == stmt && stmt->status != STMT_EXECUTING)
	{
	    self->stmts[i] = NULL;
	    ret = TRUE;
	    break;
	}
    }
    CONNLOCK_RELEASE(self);

    return ret;
}

int CC_get_max_idlen(ConnectionClass * self)
{
    int len = self->max_identifier_length;

    if (len < 0)
    {
	QResultClass *res;

	res =
	    CC_send_query(self, "show max_identifier_length", NULL,
			  ROLLBACK_ON_ERROR | IGNORE_ABORT_ON_CONN,
			  NULL);
	if (QR_command_maybe_successful(res))
	    len = self->max_identifier_length = atoi(res->command);
	QR_Destructor(res);
    }
    mylog("max_identifier_length=%d\n", len);
    return len < 0 ? 0 : len;
}

/*
 *	Create a more informative error message by concatenating the connection
 *	error message with its socket error message.
 */
static char *CC_create_errormsg(ConnectionClass * self)
{
    SocketClass *sock = self->sock;
    size_t pos;
    char msg[4096];
    const char *sockerrmsg;

    mylog("enter CC_create_errormsg\n");

    msg[0] = '\0';

    if (CC_get_errormsg(self))
	strncpy(msg, CC_get_errormsg(self), sizeof(msg));

    mylog("msg = '%s'\n", msg);

    if (sock && NULL != (sockerrmsg = SOCK_get_errmsg(sock))
	&& '\0' != sockerrmsg[0])
    {
	pos = strlen(msg);
	snprintf(&msg[pos], sizeof(msg) - pos, ";\n%s", sockerrmsg);
    }

    mylog("exit CC_create_errormsg\n");
    return msg ? strdup(msg) : NULL;
}


void
CC_set_error(ConnectionClass * self, int number, const char *message,
	     const char *func)
{
    CONNLOCK_ACQUIRE(self);
    if (self->__error_message)
	free(self->__error_message);
    self->__error_number = number;
    self->__error_message = message ? strdup(message) : NULL;
    if (0 != number)
	CC_set_error_statements(self);
    if (func && number != 0)
	CC_log_error(func, "", self);
    CONNLOCK_RELEASE(self);
}


void CC_set_errormsg(ConnectionClass * self, const char *message)
{
    CONNLOCK_ACQUIRE(self);
    if (self->__error_message)
	free(self->__error_message);
    self->__error_message = message ? strdup(message) : NULL;
    CONNLOCK_RELEASE(self);
}


char CC_get_error(ConnectionClass * self, int *number, char **message)
{
    int rv;
    char *msgcrt;

    mylog("enter CC_get_error\n");

    CONNLOCK_ACQUIRE(self);
    /* Create a very informative errormsg if it hasn't been done yet. */
    if (!self->errormsg_created)
    {
	msgcrt = CC_create_errormsg(self);
	if (self->__error_message)
	    free(self->__error_message);
	self->__error_message = msgcrt;
	self->errormsg_created = TRUE;
    }

    if (CC_get_errornumber(self))
    {
	*number = CC_get_errornumber(self);
	*message = CC_get_errormsg(self);
    }
    rv = (CC_get_errornumber(self) != 0);

    self->__error_number = 0;	/* clear the error */
    CONNLOCK_RELEASE(self);

    mylog("exit CC_get_error\n");

    return rv;
}


static void CC_clear_cursors(ConnectionClass * self, BOOL on_abort)
{
    int i;
    StatementClass *stmt;
    QResultClass *res;

    if (!self->ncursors)
	return;
    CONNLOCK_ACQUIRE(self);
    for (i = 0; i < self->num_stmts; i++)
    {
	stmt = self->stmts[i];
	if (stmt && (res = SC_get_Result(stmt)) && QR_get_cursor(res))
	{
	    if ((on_abort && !QR_is_permanent(res)) ||
		!QR_is_withhold(res))
		/*
		 * non-holdable cursors are automatically closed
		 * at commit time.
		 * all non-permanent cursors are automatically closed
		 * at rollback time.
		 */
		QR_set_cursor(res, NULL);
	    else if (!QR_is_permanent(res))
	    {
		QResultClass *wres;
		char cmd[64];

		snprintf(cmd, sizeof(cmd), "MOVE 0 in \"%s\"",
			 QR_get_cursor(res));
		CONNLOCK_RELEASE(self);
		wres =
		    CC_send_query(self, cmd, NULL,
				  ROLLBACK_ON_ERROR |
				  IGNORE_ABORT_ON_CONN, NULL);
		if (QR_command_maybe_successful(wres))
		    QR_set_permanent(res);
		else
		    QR_set_cursor(res, NULL);
		QR_Destructor(wres);
		CONNLOCK_ACQUIRE(self);
	    }
	}
    }
    CONNLOCK_RELEASE(self);
}

void CC_on_commit(ConnectionClass * conn)
{
    CONNLOCK_ACQUIRE(conn);
    if (CC_is_in_trans(conn))
    {
	CC_set_no_trans(conn);
	CC_set_no_manual_trans(conn);
    }
    CC_clear_cursors(conn, FALSE);
    CONNLOCK_RELEASE(conn);
    CC_discard_marked_objects(conn);
    CONNLOCK_ACQUIRE(conn);
    if (conn->result_uncommitted)
    {
	CONNLOCK_RELEASE(conn);
	ProcessRollback(conn, FALSE, FALSE);
	CONNLOCK_ACQUIRE(conn);
	conn->result_uncommitted = 0;
    }
    CONNLOCK_RELEASE(conn);
}

void CC_on_abort(ConnectionClass * conn, UDWORD opt)
{
    BOOL set_no_trans = FALSE;

    mylog("CC_on_abort in\n");
    CONNLOCK_ACQUIRE(conn);
    if (0 != (opt & CONN_DEAD))	/* CONN_DEAD implies NO_TRANS also */
	opt |= NO_TRANS;
    if (CC_is_in_trans(conn))
    {
	if (0 != (opt & NO_TRANS))
	{
	    CC_set_no_trans(conn);
	    CC_set_no_manual_trans(conn);
	    set_no_trans = TRUE;
	}
    }
    CC_clear_cursors(conn, TRUE);
    if (0 != (opt & CONN_DEAD))
    {
	conn->status = CONN_DOWN;
	if (conn->sock)
	{
	    CONNLOCK_RELEASE(conn);
	    SOCK_Destructor(conn->sock);
	    CONNLOCK_ACQUIRE(conn);
	    conn->sock = NULL;
	}
    } else if (set_no_trans)
    {
	CONNLOCK_RELEASE(conn);
	CC_discard_marked_objects(conn);
	CONNLOCK_ACQUIRE(conn);
    }
    if (conn->result_uncommitted)
    {
	CONNLOCK_RELEASE(conn);
	ProcessRollback(conn, TRUE, FALSE);
	CONNLOCK_ACQUIRE(conn);
	conn->result_uncommitted = 0;
    }
    CONNLOCK_RELEASE(conn);
}

void CC_on_abort_partial(ConnectionClass * conn)
{
    mylog("CC_on_abort_partial in\n");
    ProcessRollback(conn, TRUE, TRUE);
    CONNLOCK_ACQUIRE(conn);
    CC_discard_marked_objects(conn);
    CONNLOCK_RELEASE(conn);
}

static BOOL is_setting_search_path(const UCHAR * query)
{
    for (query += 4; *query; query++)
    {
	if (!isspace(*query))
	{
	    if (strnicmp((const char *)query, "search_path", 11) == 0)
		return TRUE;
	    query++;
	    while (*query && !isspace(*query))
		query++;
	}
    }
    return FALSE;
}

// VX_CLEANUP: This can't possibly have been doing anything useful.  
// But it's called from a million places, so I can't totally kill it just yet.
// Also, those million other places may want to call something non-useless.
/*
 *	The "result_in" is only used by QR_next_tuple() to fetch another group of rows into
 *	the same existing QResultClass (this occurs when the tuple cache is depleted and
 *	needs to be re-filled).
 *
 *	The "cursor" is used by SQLExecute to associate a statement handle as the cursor name
 *	(i.e., C3326857) for SQL select statements.  This cursor is then used in future
 *	'declare cursor C3326857 for ...' and 'fetch 100 in C3326857' statements.
 */
QResultClass *CC_send_query(ConnectionClass * self, char *query,
			    QueryInfo * qi, UDWORD flag,
			    StatementClass * stmt)
{
    CSTR func = "CC_send_query";
    QResultClass *cmdres = NULL, *retres = NULL, *res = NULL;

    CC_set_error(self, CONNECTION_COMMUNICATION_ERROR,
		 "CC_send_query not implemented", func);
    return NULL;
}


static char CC_setenv(ConnectionClass * self)
{
    HSTMT hstmt;
    StatementClass *stmt;
    RETCODE result;
    char status = TRUE;
    CSTR func = "CC_setenv";


    mylog("%s: entering...\n", func);

/*
 *	This function must use the local odbc API functions since the odbc state
 *	has not transitioned to "connected" yet.
 */

    result = PGAPI_AllocStmt(self, &hstmt);
    if (!SQL_SUCCEEDED(result))
	return FALSE;
    stmt = (StatementClass *) hstmt;

    stmt->internal = TRUE;	/* ensure no BEGIN/COMMIT/ABORT stuff */

    /* Set the Datestyle to the format the driver expects it to be in */
    result =
	PGAPI_ExecDirect(hstmt, (const UCHAR *)"set DateStyle to 'ISO'",
			 SQL_NTS, 0);
    if (!SQL_SUCCEEDED(result))
	status = FALSE;

    mylog("%s: result %d, status %d from set DateStyle\n", func, result,
	  status);

    /* extra_float_digits (applicable since 7.4) */
    if (PG_VERSION_GT(self, 7.3))
    {
	result =
	    PGAPI_ExecDirect(hstmt,
			     (const UCHAR *)"set extra_float_digits to 2",
			     SQL_NTS, 0);
	if (!SQL_SUCCEEDED(result))
	    status = FALSE;

	mylog("%s: result %d, status %d from set extra_float_digits\n",
	      func, result, status);

    }

    PGAPI_FreeStmt(hstmt, SQL_DROP);

    return status;
}

char CC_send_settings(ConnectionClass * self)
{
    HSTMT hstmt;
    StatementClass *stmt;
    RETCODE result;
    char status = TRUE;
#ifdef	HAVE_STRTOK_R
    char *last;
#endif				/* HAVE_STRTOK_R */
    CSTR func = "CC_send_settings";


    mylog("%s: entering...\n", func);

/*
 *	This function must use the local odbc API functions since the odbc state
 *	has not transitioned to "connected" yet.
 */

    result = PGAPI_AllocStmt(self, &hstmt);
    if (!SQL_SUCCEEDED(result))
	return FALSE;
    stmt = (StatementClass *) hstmt;

    stmt->internal = TRUE;	/* ensure no BEGIN/COMMIT/ABORT stuff */

    PGAPI_FreeStmt(hstmt, SQL_DROP);

    return status;
}


/*
 *	This function initializes the version of PostgreSQL from
 *	connInfo.protocol that we're connected to.
 *	h-inoue 01-2-2001
 */
void CC_initialize_pg_version(ConnectionClass * self)
{
    strcpy(self->pg_version, self->connInfo.protocol);
    if (PROTOCOL_62(&self->connInfo))
    {
	self->pg_version_number = (float) 6.2;
	self->pg_version_major = 6;
	self->pg_version_minor = 2;
    } else if (PROTOCOL_63(&self->connInfo))
    {
	self->pg_version_number = (float) 6.3;
	self->pg_version_major = 6;
	self->pg_version_minor = 3;
    } else if (PROTOCOL_64(&self->connInfo))
    {
	self->pg_version_number = (float) 6.4;
	self->pg_version_major = 6;
	self->pg_version_minor = 4;
    } else
    {
	self->pg_version_number = (float) 7.4;
	self->pg_version_major = 7;
	self->pg_version_minor = 4;
    }
}


/*
 *	This function gets the version of PostgreSQL that we're connected to.
 *	This is used to return the correct info in SQLGetInfo
 *	DJP - 25-1-2001
 */
static void CC_lookup_pg_version(ConnectionClass * self)
{
    HSTMT hstmt;
    StatementClass *stmt;
    RETCODE result;
    char szVersion[32];
    int major, minor;
    CSTR func = "CC_lookup_pg_version";

    mylog("%s: entering...\n", func);

/*
 *	This function must use the local odbc API functions since the odbc state
 *	has not transitioned to "connected" yet.
 */
    result = PGAPI_AllocStmt(self, &hstmt);
    if (!SQL_SUCCEEDED(result))
	return;
    stmt = (StatementClass *) hstmt;

    /* get the server's version if possible  */
    result = PGAPI_ExecDirect(hstmt, (const UCHAR *)"select version()", SQL_NTS, 0);
    if (!SQL_SUCCEEDED(result))
    {
	PGAPI_FreeStmt(hstmt, SQL_DROP);
	return;
    }

    result = PGAPI_Fetch(hstmt);
    if (!SQL_SUCCEEDED(result))
    {
	PGAPI_FreeStmt(hstmt, SQL_DROP);
	return;
    }

    result =
	PGAPI_GetData(hstmt, 1, SQL_C_CHAR, self->pg_version,
		      MAX_INFO_STRING, NULL);
    if (!SQL_SUCCEEDED(result))
    {
	PGAPI_FreeStmt(hstmt, SQL_DROP);
	return;
    }

    /*
     * Extract the Major and Minor numbers from the string. This assumes
     * the string starts 'Postgresql X.X'
     */
    strcpy(szVersion, "0.0");
    if (sscanf(self->pg_version, "%*s %d.%d", &major, &minor) >= 2)
    {
	snprintf(szVersion, sizeof(szVersion), "%d.%d", major, minor);
	self->pg_version_major = major;
	self->pg_version_minor = minor;
    }
    self->pg_version_number = (float) atof(szVersion);
    if (PG_VERSION_GE(self, 7.3))
	self->schema_support = 1;

    mylog("Got the PostgreSQL version string: '%s'\n",
	  self->pg_version);
    mylog("Extracted PostgreSQL version number: '%1.1f'\n",
	  self->pg_version_number);
    qlog("    [ PostgreSQL version string = '%s' ]\n",
	 self->pg_version);
    qlog("    [ PostgreSQL version number = '%1.1f' ]\n",
	 self->pg_version_number);

    result = PGAPI_FreeStmt(hstmt, SQL_DROP);
}


void
CC_log_error(const char *func, const char *desc,
	     const ConnectionClass * self)
{
#ifdef PRN_NULLCHECK
#define nullcheck(a) (a ? a : "(NULL)")
#endif

    if (self)
    {
	qlog("CONN ERROR: func=%s, desc='%s', errnum=%d, errmsg='%s'\n",
	     func, desc, self->__error_number,
	     nullcheck(self->__error_message));
	mylog
	    ("CONN ERROR: func=%s, desc='%s', errnum=%d, errmsg='%s'\n",
	     func, desc, self->__error_number,
	     nullcheck(self->__error_message));
	qlog("            ------------------------------------------------------------\n");
	qlog("            henv=%p, conn=%p, status=%u, num_stmts=%d\n",
	     self->henv, self, self->status, self->num_stmts);
	qlog("            sock=%p, stmts=%p\n",
	     self->sock, self->stmts);

	qlog("            ---------------- Socket Info -------------------------------\n");
	if (self->sock)
	{
	    SocketClass *sock = self->sock;

	    qlog("            socket=%d, reverse=%d, errornumber=%d, errormsg='%s'\n", sock->socket, sock->reverse, sock->errornumber, nullcheck(SOCK_get_errmsg(sock)));
	    qlog("            buffer_in=%u, buffer_out=%u\n",
		 sock->buffer_in, sock->buffer_out);
	    qlog("            buffer_filled_in=%d, buffer_filled_out=%d, buffer_read_in=%d\n", sock->buffer_filled_in, sock->buffer_filled_out, sock->buffer_read_in);
	}
    } else
    {
	qlog("INVALID CONNECTION HANDLE ERROR: func=%s, desc='%s'\n",
	     func, desc);
	mylog("INVALID CONNECTION HANDLE ERROR: func=%s, desc='%s'\n",
	      func, desc);
    }
#undef PRN_NULLCHECK
}

int CC_get_max_query_len(const ConnectionClass * conn)
{
    int value;

    /* Long Queries in 7.0+ */
    if (PG_VERSION_GE(conn, 7.0))
	value = 0 /* MAX_STATEMENT_LEN */ ;
    /* Prior to 7.0 we used 2*BLCKSZ */
    else if (PG_VERSION_GE(conn, 6.5))
	value = (2 * BLCKSZ);
    else
	/* Prior to 6.5 we used BLCKSZ */
	value = BLCKSZ;
    return value;
}

// VX_CLEANUP: This looks like a fairly cromulent thing to ask, but
// CC_send_query is the wrong way to get an answer.
/*
 *	This doesn't really return the CURRENT SCHEMA
 *	but there's no alternative.
 */
const char *CC_get_current_schema(ConnectionClass * conn)
{
    if (!conn->current_schema && conn->schema_support)
    {
	QResultClass *res;

	if (res =
	    CC_send_query(conn, "select current_schema()", NULL,
			  IGNORE_ABORT_ON_CONN | ROLLBACK_ON_ERROR,
			  NULL), QR_command_maybe_successful(res))
	{
	    if (QR_get_num_total_tuples(res) == 1)
		conn->current_schema =
		    strdup((const char *)QR_get_value_backend_text(res, 0, 0));
	}
	QR_Destructor(res);
    }
    return (const char *) conn->current_schema;
}

static int LIBPQ_send_cancel_request(const ConnectionClass * conn);
int CC_send_cancel_request(const ConnectionClass * conn)
{
    int save_errno = SOCK_ERRNO;
    SOCKETFD tmpsock = (unsigned)-1;
    struct {
	UInt4 packetlen;
	CancelRequestPacket cp;
    } crp;
    BOOL ret = TRUE;
    SocketClass *sock;
    struct sockaddr *sadr;

    /* Check we have an open connection */
    if (!conn)
	return FALSE;
    sock = CC_get_socket(conn);
    if (!sock)
	return FALSE;

    if (sock->via_libpq)
	return LIBPQ_send_cancel_request(conn);
    /*
     * We need to open a temporary connection to the postmaster. Use the
     * information saved by connectDB to do this with only kernel calls.
     */
    sadr = (struct sockaddr *) &(sock->sadr_area);
    if ((tmpsock = socket(sadr->sa_family, SOCK_STREAM, 0)) < 0)
    {
	return FALSE;
    }
    if (connect(tmpsock, sadr, sock->sadr_len) < 0)
    {
	closesocket(tmpsock);
	return FALSE;
    }

    /*
     * We needn't set nonblocking I/O or NODELAY options here.
     */
    crp.packetlen = htonl((UInt4) sizeof(crp));
    crp.cp.cancelRequestCode = (MsgType) htonl(CANCEL_REQUEST_CODE);
    crp.cp.backendPID = htonl(conn->be_pid);
    crp.cp.cancelAuthCode = htonl(conn->be_key);

    while (send(tmpsock, (char *) &crp, sizeof(crp), 0) !=
	   (int) sizeof(crp))
    {
	if (SOCK_ERRNO != EINTR)
	{
	    save_errno = SOCK_ERRNO;
	    ret = FALSE;
	    break;
	}
    }
    if (ret)
    {
	while (recv(tmpsock, (char *) &crp, 1, 0) < 0)
	{
	    if (EINTR != SOCK_ERRNO)
		break;
	}
    }

    /* Sent it, done */
    closesocket(tmpsock);
    SOCK_ERRNO_SET(save_errno);

    return ret;
}

int CC_mark_a_object_to_discard(ConnectionClass * conn, int type,
				const char *plan)
{
    int cnt = conn->num_discardp + 1;
    char *pname;

    CC_REALLOC_return_with_error(conn->discardp, char *,
				 (cnt * sizeof(char *)), conn,
				 "Couldn't alloc discardp.", -1);
    CC_MALLOC_return_with_error(pname, char, (strlen(plan) + 2), conn,
				"Couldn't alloc discardp mem.", -1);
    pname[0] = (char) type;	/* 's':prepared statement 'p':cursor */
    strcpy(pname + 1, plan);
    conn->discardp[conn->num_discardp++] = pname;

    return 1;
}

int CC_discard_marked_objects(ConnectionClass * conn)
{
    int i, cnt;
    QResultClass *res;
    char *pname, cmd[64];

    if ((cnt = conn->num_discardp) <= 0)
	return 0;
    for (i = cnt - 1; i >= 0; i--)
    {
	pname = conn->discardp[i];
	if ('s' == pname[0])
	    snprintf(cmd, sizeof(cmd), "DEALLOCATE \"%s\"", pname + 1);
	else
	    snprintf(cmd, sizeof(cmd), "CLOSE \"%s\"", pname + 1);
	res =
	    CC_send_query(conn, cmd, NULL,
			  ROLLBACK_ON_ERROR | IGNORE_ABORT_ON_CONN,
			  NULL);
	QR_Destructor(res);
	free(conn->discardp[i]);
	conn->num_discardp--;
    }

    return 1;
}

static int LIBPQ_connect(ConnectionClass * self)
{
    CSTR func = "LIBPQ_connect";
    char ret = 0;
#ifdef LIBPQ
    char *conninfo = NULL;
    void *pqconn = NULL;
    SocketClass *sock;
    int socket = -1, pqret;
    BOOL libpqLoaded;

    mylog("connecting to the database  using %s as the server\n",
	  self->connInfo.server);
    sock = self->sock;
    inolog("sock=%p\n", sock);
    if (!sock)
    {
	sock = SOCK_Constructor(self);
	if (!sock)
	{
	    CC_set_error(self, CONN_OPENDB_ERROR,
			 "Could not construct a socket to the server",
			 func);
	    goto cleanup1;
	}
    }

    if (!(conninfo = protocol3_opts_build(self)))
    {
	CC_set_error(self, CONN_OPENDB_ERROR,
		     "Couldn't allcate conninfo", func);
	goto cleanup1;
    }
    pqconn = CALL_PQconnectdb(conninfo, &libpqLoaded);
    free(conninfo);
    if (!libpqLoaded)
    {
	CC_set_error(self, CONN_OPENDB_ERROR,
		     "Couldn't load libpq library", func);
	goto cleanup1;
    }
    sock->via_libpq = TRUE;
    if (!pqconn)
    {
	CC_set_error(self, CONN_OPENDB_ERROR, "PQconnectdb error",
		     func);
	goto cleanup1;
    }
    sock->pqconn = pqconn;
    pqret = PQstatus(pqconn);
    if (CONNECTION_OK != pqret)
    {
	const char *errmsg;
	inolog("status=%d\n", pqret);
	errmsg = PQerrorMessage(pqconn);
	CC_set_error(self, CONNECTION_SERVER_NOT_REACHED, errmsg, func);
	if (CONNECTION_BAD == pqret && strstr(errmsg, "no password"))
	{
	    mylog("password retry\n");
	    PQfinish(pqconn);
	    self->sock = sock;
	    return -1;
	}
	mylog
	    ("Could not establish connection to the database; LIBPQ returned -> %s\n",
	     errmsg);
	goto cleanup1;
    }
    ret = 1;

  cleanup1:
    if (!ret)
    {
	if (sock)
	    SOCK_Destructor(sock);
	self->sock = NULL;
	return ret;
    }
    mylog("libpq connection to the database succeeded.\n");
    ret = 0;
    socket = PQsocket(pqconn);
    inolog("socket=%d\n", socket);
    sock->socket = socket;
    sock->ssl = PQgetssl(pqconn);
    if (TRUE)
    {
	int pversion;
	ConnInfo *ci = &self->connInfo;

	sock->pversion = PG_PROTOCOL_74;
	strncpy(ci->protocol, PG74, sizeof(ci->protocol));
	pversion = PQprotocolVersion(pqconn);
	switch (pversion)
	{
	case 2:
	    sock->pversion = PG_PROTOCOL_64;
	    strncpy(ci->protocol, PG64, sizeof(ci->protocol));
	    break;
	}
    }
    mylog("procotol=%s\n", self->connInfo.protocol);
    {
	int pversion, on;

	pversion = PQserverVersion(pqconn);
	self->pg_version_major = pversion / 10000;
	self->pg_version_minor = (pversion % 10000) / 100;
	sprintf(self->pg_version, "%d.%d.%d", self->pg_version_major,
		self->pg_version_minor, pversion % 100);
	self->pg_version_number = (float) atof(self->pg_version);
	if (PG_VERSION_GE(self, 7.3))
	    self->schema_support = 1;
	/* blocking mode */
	/* ioctlsocket(sock, FIONBIO , 0);
	   setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *) &on, sizeof(on)); */
    }
    if (sock->ssl)
    {
	/* flags = fcntl(sock, F_GETFL);
	   fcntl(sock, F_SETFL, flags & (~O_NONBLOCKING)); */
    }
    mylog("Server version=%s\n", self->pg_version);
    ret = 1;
    if (ret)
    {
	self->sock = sock;
	if (!CC_get_username(self)[0])
	{
	    mylog("PQuser=%s\n", PQuser(pqconn));
	    strcpy(self->connInfo.username, PQuser(pqconn));
	}
    } else
    {
	SOCK_Destructor(sock);
	self->sock = NULL;
    }
#else
    ret = -1;
#endif
    mylog("%s: retuning %d\n", func, ret);
    return ret;
}

static int LIBPQ_send_cancel_request(const ConnectionClass * conn)
{
#ifdef LIBPQ
    int ret = 0;
    char errbuf[256];
    void *cancel;
    SocketClass *sock = CC_get_socket(conn);

    if (!sock)
	return FALSE;

    cancel = PQgetCancel(sock->pqconn);
    if (!cancel)
	return FALSE;
    ret = PQcancel(cancel, errbuf, sizeof(errbuf));
    PQfreeCancel(cancel);
    if (1 == ret)
	return TRUE;
    else
#endif
	return FALSE;
}

const char *CurrCat(const ConnectionClass * conn)
{
    if (conn->schema_support)
	return conn->connInfo.database;
    else
	return NULL;
}

const char *CurrCatString(const ConnectionClass * conn)
{
    const char *cat = CurrCat(conn);

    if (!cat)
	cat = NULL_STRING;
    return cat;
}
