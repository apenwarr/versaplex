/*
 * Description:	This module contains routines related to
 *		preparing and executing an SQL statement.
 */

#include "psqlodbc.h"
#include "misc.h"

#include <stdio.h>
#include <string.h>

#include <ctype.h>

#include "environ.h"
#include "connection.h"
#include "statement.h"
#include "qresult.h"
#include "convert.h"
#include "bind.h"
#include "pgtypes.h"
#include "pgapifunc.h"
#include "vxhelpers.h"

/*extern GLOBAL_VALUES globals;*/


/*		Perform a Prepare on the SQL statement */
RETCODE SQL_API
PGAPI_Prepare(HSTMT hstmt,
	      const SQLCHAR FAR * szSqlStr, SQLINTEGER cbSqlStr)
{
    CSTR func = "PGAPI_Prepare";
    StatementClass *self = (StatementClass *) hstmt;
    RETCODE retval = SQL_SUCCESS;

    mylog("%s: entering...\n", func);

#define	return	DONT_CALL_RETURN_FROM_HERE???
    /* StartRollbackState(self); */
    if (!self)
    {
	SC_log_error(func, "", NULL);
	retval = SQL_INVALID_HANDLE;
	goto cleanup;
    }

    /*
     * According to the ODBC specs it is valid to call SQLPrepare multiple
     * times. In that case, the bound SQL statement is replaced by the new
     * one
     */

    SC_set_prepared(self, NOT_YET_PREPARED);
    switch (self->status)
    {
    case STMT_PREMATURE:
	mylog("**** PGAPI_Prepare: STMT_PREMATURE, recycle\n");
	SC_recycle_statement(self);	/* recycle the statement, but do
					 * not remove parameter bindings */
	break;

    case STMT_FINISHED:
	mylog("**** PGAPI_Prepare: STMT_FINISHED, recycle\n");
	SC_recycle_statement(self);	/* recycle the statement, but do
					 * not remove parameter bindings */
	break;

    case STMT_ALLOCATED:
	mylog("**** PGAPI_Prepare: STMT_ALLOCATED, copy\n");
	self->status = STMT_READY;
	break;

    case STMT_READY:
	mylog("**** PGAPI_Prepare: STMT_READY, change SQL\n");
	break;

    case STMT_EXECUTING:
	mylog("**** PGAPI_Prepare: STMT_EXECUTING, error!\n");

	SC_set_error(self, STMT_SEQUENCE_ERROR,
		     "PGAPI_Prepare(): The handle does not point to a statement that is ready to be executed",
		     func);

	retval = SQL_ERROR;
	goto cleanup;

    default:
	SC_set_error(self, STMT_INTERNAL_ERROR,
		     "An Internal Error has occured -- Unknown statement status.",
		     func);
	retval = SQL_ERROR;
	goto cleanup;
    }

    SC_initialize_stmts(self, TRUE);

    if (!szSqlStr)
    {
	SC_set_error(self, STMT_NO_MEMORY_ERROR, "the query is NULL",
		     func);
	retval = SQL_ERROR;
	goto cleanup;
    }
    if (!szSqlStr[0])
	self->statement = strdup("");
    else
	self->statement = make_string(szSqlStr, cbSqlStr, NULL, 0);
    if (!self->statement)
    {
	SC_set_error(self, STMT_NO_MEMORY_ERROR,
		     "No memory available to store statement", func);
	retval = SQL_ERROR;
	goto cleanup;
    }

    self->prepare = PREPARE_STATEMENT;
    self->statement_type = statement_type(self->statement);

    /* Check if connection is onlyread (only selects are allowed) */
    if (CC_is_onlyread(SC_get_conn(self)) && STMT_UPDATE(self))
    {
	SC_set_error(self, STMT_EXEC_ERROR,
		     "Connection is readonly, only select statements are allowed.",
		     func);
	retval = SQL_ERROR;
	goto cleanup;
    }

  cleanup:
#undef	return
    inolog("SQLPrepare return=%d\n", retval);
    if (self->internal)
	retval = DiscardStatementSvp(self, retval, FALSE);
    return retval;
}



// VX_CLEANUP: Anything calling this is obviously broken, but it's not so
// obvious whether the callers should be fixed to call PGAPI_ExecDirect_Vx or
// killed.
/*		Performs the equivalent of SQLPrepare, followed by SQLExecute. */
RETCODE SQL_API
PGAPI_ExecDirect(HSTMT hstmt,
		 const SQLCHAR FAR * szSqlStr,
		 SQLINTEGER cbSqlStr, UWORD flag)
{
    StatementClass *stmt = (StatementClass *) hstmt;
    RETCODE result;
    CSTR func = "PGAPI_ExecDirect";
    SC_set_error(stmt, STMT_EXEC_ERROR,
		 "PGAPI_ExecDirect is not supported, call PGAPI_ExecDirect_Vx", 
		 func);
    return SQL_ERROR;
}


/* Performs the equivalent of SQLPrepare, followed by SQLExecute. */
RETCODE SQL_API
PGAPI_ExecDirect_Vx(HSTMT hstmt,
		 const SQLCHAR FAR * szSqlStr,
		 SQLINTEGER cbSqlStr, UWORD flag)
{
    StatementClass *stmt = (StatementClass *)hstmt;

    VxStatement st(stmt);
    VxResultSet rs;
    st.reinit();
    rs.runquery(st.dbus(), "ExecRecordset", (const char *)szSqlStr);
    st.set_result(rs);
    stmt->statement = strdup((const char *)szSqlStr);
    stmt->catalog_result = FALSE;
    stmt->statement_type = STMT_TYPE_SELECT;
//    SC_set_parse_forced(stmt);

cleanup:
    return st.retcode();
}


/*	Execute a prepared SQL statement */
RETCODE SQL_API PGAPI_Execute_Vx(HSTMT hstmt, UWORD flag)
{
    StatementClass *stmt = (StatementClass *)hstmt;

    VxStatement st(stmt);
    VxResultSet rs;
    rs.runquery(st.dbus(), "ExecRecordset", stmt->statement);
    st.set_result(rs);

cleanup:
    return st.retcode();
}


static int inquireHowToPrepare(const StatementClass * stmt)
{
    ConnectionClass *conn;
    ConnInfo *ci;
    int ret = 0;

    conn = SC_get_conn(stmt);
    ci = &(conn->connInfo);
    if (!ci->use_server_side_prepare || PG_VERSION_LT(conn, 7.3))
    {
	/* Do prepare operations by the driver itself */
	return PREPARE_BY_THE_DRIVER;
    }
    if (NOT_YET_PREPARED == stmt->prepared)
    {
	SQLSMALLINT num_params;

	if (STMT_TYPE_DECLARE == stmt->statement_type &&
	    PG_VERSION_LT(conn, 8.0))
	{
	    return PREPARE_BY_THE_DRIVER;
	}
	if (stmt->multi_statement < 0)
	    PGAPI_NumParams((StatementClass *) stmt, &num_params);
	if (stmt->multi_statement > 0)	/* would divide the query into multiple commands and apply V3 parse requests for each of them */
	    ret = PARSE_REQ_FOR_INFO;
	else if (PROTOCOL_74(ci))
	{
	    if (STMT_TYPE_SELECT == stmt->statement_type)
	    {
		if (SQL_CURSOR_FORWARD_ONLY !=
			 stmt->options.cursor_type)
		    ret = PARSE_REQ_FOR_INFO;
		else
		    ret = PARSE_TO_EXEC_ONCE;
	    } else
		ret = PARSE_TO_EXEC_ONCE;
	} else
	{
	    if (STMT_TYPE_SELECT == stmt->statement_type &&
		(SQL_CURSOR_FORWARD_ONLY != stmt->options.cursor_type))
		ret = PREPARE_BY_THE_DRIVER;
	    else
		ret = USING_PREPARE_COMMAND;
	}
    }
    if (SC_is_prepare_statement(stmt) && (PARSE_TO_EXEC_ONCE == ret))
	ret = NAMED_PARSE_REQUEST;

    return ret;
}


// VX_CLEANUP: This looks fishy
int StartRollbackState(StatementClass * stmt)
{
    CSTR func = "StartRollbackState";
    int ret;
    ConnectionClass *conn;
    ConnInfo *ci = NULL;

    inolog("%s:%p->internal=%d\n", func, stmt, stmt->internal);
    conn = SC_get_conn(stmt);
    if (conn)
	ci = &conn->connInfo;
    ret = 0;
    if (!ci || ci->rollback_on_error < 0)	/* default */
    {
	if (conn && PG_VERSION_GE(conn, 8.0))
	    ret = 2;		/* statement rollback */
	else
	    ret = 1;		/* transaction rollback */
    } else
    {
	ret = ci->rollback_on_error;
	if (2 == ret && PG_VERSION_LT(conn, 8.0))
	    ret = 1;
    }
    switch (ret)
    {
    case 1:
	SC_start_tc_stmt(stmt);
	break;
    case 2:
	SC_start_rb_stmt(stmt);
	break;
    }
    return ret;
}

/*
 *	Must be in a transaction or the subsequent execution
 *	invokes a transaction.
 */
RETCODE SetStatementSvp(StatementClass * stmt)
{
    CSTR func = "SetStatementSvp";
    char esavepoint[32], cmd[64];
    ConnectionClass *conn = SC_get_conn(stmt);
    QResultClass *res;
    RETCODE ret = SQL_SUCCESS_WITH_INFO;

    if (CC_is_in_error_trans(conn))
	return ret;

    if (0 == stmt->lock_CC_for_rb)
    {
	ENTER_CONN_CS(conn);
	stmt->lock_CC_for_rb++;
    }
    switch (stmt->statement_type)
    {
    case STMT_TYPE_SPECIAL:
    case STMT_TYPE_TRANSACTION:
	return ret;
    }
    if (!SC_accessed_db(stmt))
    {
	BOOL need_savep = FALSE;

	if (stmt->internal)
	{
	    if (PG_VERSION_GE(conn, 8.0))
		SC_start_rb_stmt(stmt);
	    else
		SC_start_tc_stmt(stmt);
	}
	if (SC_is_rb_stmt(stmt))
	{
	    if (CC_is_in_trans(conn))
	    {
		need_savep = TRUE;
	    }
	}
	if (need_savep)
	{
	    sprintf(esavepoint, "_EXEC_SVP_%p", stmt);
	    snprintf(cmd, sizeof(cmd), "SAVEPOINT %s", esavepoint);
	    res = CC_send_query(conn, cmd, NULL, 0, NULL);
	    if (QR_command_maybe_successful(res))
	    {
		SC_set_accessed_db(stmt);
		SC_start_rbpoint(stmt);
		ret = SQL_SUCCESS;
	    } else
	    {
		SC_set_error(stmt, STMT_INTERNAL_ERROR,
			     "internal SAVEPOINT failed", func);
		ret = SQL_ERROR;
	    }
	    QR_Destructor(res);
	} else
	    SC_set_accessed_db(stmt);
    }
    inolog("%s:%p->accessed=%d\n", func, stmt, SC_accessed_db(stmt));
    return ret;
}

RETCODE
DiscardStatementSvp(StatementClass * stmt, RETCODE ret, BOOL errorOnly)
{
    CSTR func = "DiscardStatementSvp";
    char esavepoint[32], cmd[64];
    ConnectionClass *conn = SC_get_conn(stmt);
    QResultClass *res;
    BOOL cmd_success, start_stmt = FALSE;
    mylog("Hello\n");

    inolog("%s:%p->accessed=%d is_in=%d is_rb=%d is_tc=%d\n", func,
	   stmt, SC_accessed_db(stmt), CC_is_in_trans(conn),
	   SC_is_rb_stmt(stmt), SC_is_tc_stmt(stmt));
    switch (ret)
    {
    case SQL_NEED_DATA:
	break;
    case SQL_ERROR:
	start_stmt = TRUE;
	break;
    default:
	if (!errorOnly)
	    start_stmt = TRUE;
	break;
    }
    if (!SC_accessed_db(stmt) || !CC_is_in_trans(conn))
	goto cleanup;
    if (!SC_is_rb_stmt(stmt) && !SC_is_tc_stmt(stmt))
	goto cleanup;
    sprintf(esavepoint, "_EXEC_SVP_%p", stmt);
    if (SQL_ERROR == ret)
    {
	if (SC_started_rbpoint(stmt))
	{
	    snprintf(cmd, sizeof(cmd), "ROLLBACK to %s", esavepoint);
	    res =
		CC_send_query(conn, cmd, NULL, IGNORE_ABORT_ON_CONN,
			      NULL);
	    cmd_success = QR_command_maybe_successful(res);
	    QR_Destructor(res);
	    if (!cmd_success)
	    {
		SC_set_error(stmt, STMT_INTERNAL_ERROR,
			     "internal ROLLBACK failed", func);
		CC_abort(conn);
		goto cleanup;
	    }
	} else
	{
	    CC_abort(conn);
	    goto cleanup;
	}
    } else if (errorOnly)
	return ret;
    inolog("ret=%d\n", ret);
    if (SQL_NEED_DATA != ret && SC_started_rbpoint(stmt))
    {
	snprintf(cmd, sizeof(cmd), "RELEASE %s", esavepoint);
	res =
	    CC_send_query(conn, cmd, NULL, IGNORE_ABORT_ON_CONN, NULL);
	cmd_success = QR_command_maybe_successful(res);
	QR_Destructor(res);
	if (!cmd_success)
	{
	    SC_set_error(stmt, STMT_INTERNAL_ERROR,
			 "internal RELEASE failed", func);
	    CC_abort(conn);
	    ret = SQL_ERROR;
	}
    }
  cleanup:
    if (SQL_NEED_DATA != ret)
	SC_forget_unnamed(stmt);	/* unnamed plan is no longer reliable */
    if (!SC_is_prepare_statement(stmt)
	&& ONCE_DESCRIBED == stmt->prepared)
	SC_set_prepared(stmt, NOT_YET_PREPARED);
    if (start_stmt || SQL_ERROR == ret)
    {
	if (stmt->lock_CC_for_rb > 0)
	{
	    LEAVE_CONN_CS(conn);
	    stmt->lock_CC_for_rb--;
	}
	SC_start_stmt(stmt);
    }
    return ret;
}

void SC_setInsertedTable(StatementClass * stmt, RETCODE retval)
{
    const char *cmd = stmt->statement, *ptr;
    ConnectionClass *conn;
    size_t len;

    if (STMT_TYPE_INSERT != stmt->statement_type)
	return;
    if (SQL_NEED_DATA == retval)
	return;
    conn = SC_get_conn(stmt);
    if (PG_VERSION_GE(conn, 8.1))	/* lastval() is available */
	return;
    /*if (!CC_fake_mss(conn))
       return; */
    while (isspace((UCHAR) * cmd))
	cmd++;
    if (!*cmd)
	return;
    len = 6;
    if (strnicmp(cmd, "insert", len))
	return;
    cmd += len;
    while (isspace((UCHAR) * (++cmd)));
    if (!*cmd)
	return;
    len = 4;
    if (strnicmp(cmd, "into", len))
	return;
    cmd += len;
    while (isspace((UCHAR) * (++cmd)));
    if (!*cmd)
	return;
    NULL_THE_NAME(conn->schemaIns);
    NULL_THE_NAME(conn->tableIns);
    if (!SQL_SUCCEEDED(retval))
	return;
    ptr = NULL;
    if (IDENTIFIER_QUOTE == *cmd)
    {
	if (ptr = strchr(cmd + 1, IDENTIFIER_QUOTE), NULL == ptr)
	    return;
	if ('.' == ptr[1])
	{
	    len = ptr - cmd - 1;
	    STRN_TO_NAME(conn->schemaIns, cmd + 1, len);
	    cmd = ptr + 2;
	    ptr = NULL;
	}
    } else
    {
	if (ptr = strchr(cmd + 1, '.'), NULL != ptr)
	{
	    len = ptr - cmd;
	    STRN_TO_NAME(conn->schemaIns, cmd, len);
	    cmd = ptr + 1;
	    ptr = NULL;
	}
    }
    if (IDENTIFIER_QUOTE == *cmd && NULL == ptr)
    {
	if (ptr = strchr(cmd + 1, IDENTIFIER_QUOTE), NULL == ptr)
	    return;
    }
    if (IDENTIFIER_QUOTE == *cmd)
    {
	len = ptr - cmd - 1;
	STRN_TO_NAME(conn->tableIns, cmd + 1, len);
    } else
    {
	ptr = cmd;
	while (*ptr && !isspace((UCHAR) * ptr))
	    ptr++;
	len = ptr - cmd;
	STRN_TO_NAME(conn->tableIns, cmd, len);
    }
}

/*	Execute a prepared SQL statement */
RETCODE SQL_API PGAPI_Execute(HSTMT hstmt, UWORD flag)
{
    CSTR func = "PGAPI_Execute";
    StatementClass *stmt = (StatementClass *) hstmt;
    SC_set_error(stmt, STMT_EXEC_ERROR,
		 "PGAPI_Execute is not supported, call PGAPI_ExecDirect_Vx", 
		 func);
    return SQL_ERROR;
}


RETCODE SQL_API PGAPI_Transact(HENV henv, HDBC hdbc, SQLUSMALLINT fType)
{
    CSTR func = "PGAPI_Transact";
    extern ConnectionClass *conns[];
    ConnectionClass *conn;
    QResultClass *res;
    char ok, *stmt_string;
    int lf;

    mylog("entering %s: hdbc=%p, henv=%p\n", func, hdbc, henv);

    if (hdbc == SQL_NULL_HDBC && henv == SQL_NULL_HENV)
    {
	CC_log_error(func, "", NULL);
	return SQL_INVALID_HANDLE;
    }

    /*
     * If hdbc is null and henv is valid, it means transact all
     * connections on that henv.
     */
    if (hdbc == SQL_NULL_HDBC && henv != SQL_NULL_HENV)
    {
	for (lf = 0; lf < MAX_CONNECTIONS; lf++)
	{
	    conn = conns[lf];

	    if (conn && conn->henv == henv)
		if (PGAPI_Transact(henv, (HDBC) conn, fType) !=
		    SQL_SUCCESS)
		    return SQL_ERROR;
	}
	return SQL_SUCCESS;
    }

    conn = (ConnectionClass *) hdbc;

    if (fType == SQL_COMMIT)
	stmt_string = "COMMIT";
    else if (fType == SQL_ROLLBACK)
	stmt_string = "ROLLBACK";
    else
    {
	CC_set_error(conn, CONN_INVALID_ARGUMENT_NO,
		     "PGAPI_Transact can only be called with SQL_COMMIT or SQL_ROLLBACK as parameter",
		     func);
	return SQL_ERROR;
    }

    /* If manual commit and in transaction, then proceed. */
    if (!CC_is_in_autocommit(conn) && CC_is_in_trans(conn))
    {
	mylog("PGAPI_Transact: sending on conn %d '%s'\n", conn,
	      stmt_string);

	res = CC_send_query(conn, stmt_string, NULL, 0, NULL);
	ok = QR_command_maybe_successful(res);
	QR_Destructor(res);
	if (!ok)
	{
	    /* error msg will be in the connection */
	    CC_on_abort(conn, NO_TRANS);
	    CC_log_error(func, "", conn);
	    return SQL_ERROR;
	}
    }
    return SQL_SUCCESS;
}


RETCODE SQL_API PGAPI_Cancel(HSTMT hstmt)	/* Statement to cancel. */
{
    CSTR func = "PGAPI_Cancel";
    StatementClass *stmt = (StatementClass *) hstmt, *estmt;
    ConnectionClass *conn;
    RETCODE ret = SQL_SUCCESS;
    BOOL entered_cs = FALSE;
    ConnInfo *ci;

    mylog("%s: entering...\n", func);

    /* Check if this can handle canceling in the middle of a SQLPutData? */
    if (!stmt)
    {
	SC_log_error(func, "", NULL);
	return SQL_INVALID_HANDLE;
    }
    conn = SC_get_conn(stmt);
    ci = &(conn->connInfo);

#define	return	DONT_CALL_RETURN_FROM_HERE???
    /* StartRollbackState(stmt); */

    if (stmt->execute_delegate)
	estmt = stmt->execute_delegate;
    else
	estmt = stmt;
    /*
     * Not in the middle of SQLParamData/SQLPutData so cancel like a
     * close.
     */
    if (estmt->data_at_exec < 0)
    {
	/*
	 * Tell the Backend that we're cancelling this request
	 */
	if (estmt->status == STMT_EXECUTING)
	{
	    if (!CC_send_cancel_request(conn))
	    {
		ret = SQL_ERROR;
	    }
	    goto cleanup;
	}
	/*
	 * MAJOR HACK for Windows to reset the driver manager's cursor
	 * state: Because of what seems like a bug in the Odbc driver
	 * manager, SQLCancel does not act like a SQLFreeStmt(CLOSE), as
	 * many applications depend on this behavior.  So, this brute
	 * force method calls the driver manager's function on behalf of
	 * the application.
	 */

	if (conn->driver_version < 0x0350)
	{
	    ENTER_STMT_CS(stmt);
	    entered_cs = TRUE;
	    SC_clear_error(stmt);
	    ret = PGAPI_FreeStmt(hstmt, SQL_CLOSE);

	    mylog("PGAPI_Cancel:  PGAPI_FreeStmt returned %d\n", ret);
	}
	goto cleanup;
    }

    /* In the middle of SQLParamData/SQLPutData, so cancel that. */
    /*
     * Note, any previous data-at-exec buffers will be freed in the
     * recycle
     */
    /* if they call SQLExecDirect or SQLExecute again. */

    ENTER_STMT_CS(stmt);
    entered_cs = TRUE;
    SC_clear_error(stmt);
    estmt->data_at_exec = -1;
    estmt->current_exec_param = -1;
    estmt->put_data = FALSE;
    cancelNeedDataState(estmt);

  cleanup:
#undef	return
    if (entered_cs)
    {
	if (stmt->internal)
	    ret = DiscardStatementSvp(stmt, ret, FALSE);
	LEAVE_STMT_CS(stmt);
    }
    return ret;
}


/*
 *	Returns the SQL string as modified by the driver.
 *	Currently, just copy the input string without modification
 *	observing buffer limits and truncation.
 */
RETCODE SQL_API
PGAPI_NativeSql(HDBC hdbc,
		const SQLCHAR FAR * szSqlStrIn,
		SQLINTEGER cbSqlStrIn,
		SQLCHAR FAR * szSqlStr,
		SQLINTEGER cbSqlStrMax, SQLINTEGER FAR * pcbSqlStr)
{
    CSTR func = "PGAPI_NativeSql";
    size_t len = 0;
    char *ptr;
    ConnectionClass *conn = (ConnectionClass *) hdbc;
    RETCODE result;

    mylog("%s: entering...cbSqlStrIn=%d\n", func, cbSqlStrIn);

    ptr = (char *)(
	(cbSqlStrIn == 0) ? "" : make_string(szSqlStrIn, cbSqlStrIn,
					     NULL, 0));
    if (!ptr)
    {
	CC_set_error(conn, CONN_NO_MEMORY_ERROR,
		     "No memory available to store native sql string",
		     func);
	return SQL_ERROR;
    }

    result = SQL_SUCCESS;
    len = strlen(ptr);

    if (szSqlStr)
    {
	strncpy_null((char *)szSqlStr, ptr, cbSqlStrMax);

	if (len >= cbSqlStrMax)
	{
	    result = SQL_SUCCESS_WITH_INFO;
	    CC_set_error(conn, CONN_TRUNCATED,
			 "The buffer was too small for the NativeSQL.",
			 func);
	}
    }

    if (pcbSqlStr)
	*pcbSqlStr = (SQLINTEGER) len;

    if (cbSqlStrIn)
	free(ptr);

    return result;
}


/*
 *	Supplies parameter data at execution time.
 *	Used in conjuction with SQLPutData.
 */
RETCODE SQL_API PGAPI_ParamData(HSTMT hstmt, PTR FAR * prgbValue)
{
    CSTR func = "PGAPI_ParamData";
    StatementClass *stmt = (StatementClass *) hstmt, *estmt;
    SC_log_error(func, "PGAPI_ParamData is not yet implemented", stmt);
    return SQL_ERROR;
}


/*
 *	Supplies parameter data at execution time.
 *	Used in conjunction with SQLParamData.
 */
RETCODE SQL_API PGAPI_PutData(HSTMT hstmt, PTR rgbValue, SQLLEN cbValue)
{
    CSTR func = "PGAPI_PutData";
    StatementClass *stmt = (StatementClass *) hstmt, *estmt;
    ConnectionClass *conn;
    RETCODE retval = SQL_SUCCESS;
    APDFields *apdopts;
    IPDFields *ipdopts;
    SQLLEN old_pos;
    ParameterInfoClass *current_param;
    ParameterImplClass *current_iparam;
    PutDataClass *current_pdata;
    char *buffer, *putbuf, *allocbuf = NULL;
    Int2 ctype;
    SQLLEN putlen;
    BOOL lenset = FALSE;

    mylog("%s: entering...\n", func);

#define	return	DONT_CALL_RETURN_FROM_HERE???
    if (!stmt)
    {
	SC_log_error(func, "", NULL);
	retval = SQL_INVALID_HANDLE;
	goto cleanup;
    }
    if (SC_AcceptedCancelRequest(stmt))
    {
	SC_set_error(stmt, STMT_OPERATION_CANCELLED,
		     "Cancel the statement, sorry.", func);
	retval = SQL_ERROR;
	goto cleanup;
    }

    estmt = stmt->execute_delegate ? stmt->execute_delegate : stmt;
    apdopts = SC_get_APDF(estmt);
    if (estmt->current_exec_param < 0)
    {
	SC_set_error(stmt, STMT_SEQUENCE_ERROR,
		     "Previous call was not SQLPutData or SQLParamData",
		     func);
	retval = SQL_ERROR;
	goto cleanup;
    }

    current_param = &(apdopts->parameters[estmt->current_exec_param]);
    ipdopts = SC_get_IPDF(estmt);
    current_iparam = &(ipdopts->parameters[estmt->current_exec_param]);
    ctype = current_param->CType;

    conn = SC_get_conn(estmt);
    if (ctype == SQL_C_DEFAULT)
    {
	ctype = sqltype_to_default_ctype(conn, current_iparam->SQLType);
	if (SQL_C_WCHAR == ctype && CC_default_is_c(conn))
	    ctype = SQL_C_CHAR;
    }
    if (SQL_NTS == cbValue)
    {
#ifdef	UNICODE_SUPPORT
	if (SQL_C_WCHAR == ctype)
	{
	    putlen = WCLEN * ucs2strlen((SQLWCHAR *) rgbValue);
	    lenset = TRUE;
	} else
#endif				/* UNICODE_SUPPORT */
	if (SQL_C_CHAR == ctype)
	{
	    putlen = strlen((const char *)rgbValue);
	    lenset = TRUE;
	}
    }
    if (!lenset)
    {
	if (cbValue < 0)
	    putlen = cbValue;
	else
#ifdef	UNICODE_SUPPORT
	if (ctype == SQL_C_CHAR || ctype == SQL_C_BINARY
		|| ctype == SQL_C_WCHAR)
#else
	if (ctype == SQL_C_CHAR || ctype == SQL_C_BINARY)
#endif				/* UNICODE_SUPPORT */
	    putlen = cbValue;
	else
	    putlen = ctype_length(ctype);
    }
    putbuf = (char *)rgbValue;

    if (!estmt->put_data)
    {				/* first call */
	mylog("PGAPI_PutData: (1) cbValue = %d\n", cbValue);

	estmt->put_data = TRUE;

	current_pdata->EXEC_used = (SQLLEN *) malloc(sizeof(SQLLEN));
	if (!current_pdata->EXEC_used)
	{
	    SC_set_error(stmt, STMT_NO_MEMORY_ERROR,
			 "Out of memory in PGAPI_PutData (1)", func);
	    retval = SQL_ERROR;
	    goto cleanup;
	}

	*current_pdata->EXEC_used = putlen;

	if (cbValue == SQL_NULL_DATA)
	{
	    retval = SQL_SUCCESS;
	    goto cleanup;
	}

	current_pdata->EXEC_buffer = (char *)malloc(putlen + 1);
	if (!current_pdata->EXEC_buffer)
	{
	    SC_set_error(stmt, STMT_NO_MEMORY_ERROR,
			 "Out of memory in PGAPI_PutData (2)",
			 func);
	    retval = SQL_ERROR;
	    goto cleanup;
	}
	memcpy(current_pdata->EXEC_buffer, putbuf, putlen);
	current_pdata->EXEC_buffer[putlen] = '\0';
    } else
    {
	/* calling SQLPutData more than once */
	mylog("PGAPI_PutData: (>1) cbValue = %d\n", cbValue);

	buffer = current_pdata->EXEC_buffer;
	old_pos = *current_pdata->EXEC_used;
	if (putlen > 0)
	{
	    SQLLEN used =
		*current_pdata->EXEC_used + putlen, allocsize;
	    for (allocsize = (1 << 4); allocsize <= used;
		 allocsize <<= 1);
	    mylog
		("        cbValue = %d, old_pos = %d, *used = %d\n",
		 putlen, old_pos, used);

	    /* dont lose the old pointer in case out of memory */
	    buffer = (char *)realloc(current_pdata->EXEC_buffer, allocsize);
	    if (!buffer)
	    {
		SC_set_error(stmt, STMT_NO_MEMORY_ERROR,
			     "Out of memory in PGAPI_PutData (3)",
			     func);
		retval = SQL_ERROR;
		goto cleanup;
	    }

	    memcpy(&buffer[old_pos], putbuf, putlen);
	    buffer[used] = '\0';

	    /* reassign buffer incase realloc moved it */
	    *current_pdata->EXEC_used = used;
	    current_pdata->EXEC_buffer = buffer;
	} else
	{
	    SC_set_error(stmt, STMT_INTERNAL_ERROR, "bad cbValue",
			 func);
	    retval = SQL_ERROR;
	    goto cleanup;
	}
    }

    retval = SQL_SUCCESS;
  cleanup:
#undef	return
    if (allocbuf)
	free(allocbuf);
    if (stmt->internal)
	retval = DiscardStatementSvp(stmt, retval, TRUE);

    return retval;
}
