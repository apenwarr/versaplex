/*
 * Description:		This module contains routines related to ODBC 3.0
 *			most of their implementations are temporary
 *			and must be rewritten properly.
 *			2001/07/23	inoue
 */

#include "psqlodbc.h"
#include "misc.h"

#include <stdio.h>
#include <string.h>

#include "environ.h"
#include "connection.h"
#include "statement.h"
#include "descriptor.h"
#include "qresult.h"
#include "pgapifunc.h"


/*	SQLError -> SQLDiagRec */
RETCODE SQL_API
PGAPI_GetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle,
		 SQLSMALLINT RecNumber, SQLCHAR * Sqlstate,
		 SQLINTEGER * NativeError, SQLCHAR * MessageText,
		 SQLSMALLINT BufferLength, SQLSMALLINT * TextLength)
{
    RETCODE ret;
    CSTR func = "PGAPI_GetDiagRec";

    mylog("%s entering type=%d rec=%d\n", func, HandleType, RecNumber);
    switch (HandleType)
    {
    case SQL_HANDLE_ENV:
	ret = PGAPI_EnvError(Handle, RecNumber, Sqlstate,
			     NativeError, MessageText,
			     BufferLength, TextLength, 0);
	break;
    case SQL_HANDLE_DBC:
	ret = PGAPI_ConnectError(Handle, RecNumber, Sqlstate,
				 NativeError, MessageText, BufferLength,
				 TextLength, 0);
	break;
    case SQL_HANDLE_STMT:
	ret = PGAPI_StmtError(Handle, RecNumber, Sqlstate,
			      NativeError, MessageText, BufferLength,
			      TextLength, 0);
	break;
    case SQL_HANDLE_DESC:
	ret = PGAPI_DescError(Handle, RecNumber, Sqlstate,
			      NativeError,
			      MessageText, BufferLength, TextLength, 0);
	break;
    default:
	ret = SQL_ERROR;
    }
    mylog("%s exiting %d\n", func, ret);
    return ret;
}

/*
 *	Minimal implementation. 
 *
 */
RETCODE SQL_API
PGAPI_GetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle,
		   SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier,
		   PTR DiagInfoPtr, SQLSMALLINT BufferLength,
		   SQLSMALLINT * StringLengthPtr)
{
    RETCODE ret = SQL_ERROR, rtn;
    ConnectionClass *conn;
    StatementClass *stmt;
    SQLLEN rc;
    SQLSMALLINT pcbErrm;
    ssize_t rtnlen = -1;
    int rtnctype = SQL_C_CHAR;
    CSTR func = "PGAPI_GetDiagField";

    mylog("%s entering rec=%d", func, RecNumber);
    switch (HandleType)
    {
    case SQL_HANDLE_ENV:
	switch (DiagIdentifier)
	{
	case SQL_DIAG_CLASS_ORIGIN:
	case SQL_DIAG_SUBCLASS_ORIGIN:
	case SQL_DIAG_CONNECTION_NAME:
	case SQL_DIAG_SERVER_NAME:
	    rtnlen = 0;
	    if (DiagInfoPtr && BufferLength > rtnlen)
	    {
		ret = SQL_SUCCESS;
		*((char *) DiagInfoPtr) = '\0';
	    } else
		ret = SQL_SUCCESS_WITH_INFO;
	    break;
	case SQL_DIAG_MESSAGE_TEXT:
	    ret = PGAPI_EnvError(Handle, RecNumber,
				 NULL, NULL, (UCHAR *)DiagInfoPtr,
				 BufferLength, StringLengthPtr, 0);
	    break;
	case SQL_DIAG_NATIVE:
	    rtnctype = SQL_C_LONG;
	    ret = PGAPI_EnvError(Handle, RecNumber,
				 NULL, (SQLINTEGER *) DiagInfoPtr, NULL,
				 0, NULL, 0);
	    break;
	case SQL_DIAG_NUMBER:
	    rtnctype = SQL_C_LONG;
	    ret = PGAPI_EnvError(Handle, RecNumber,
				 NULL, NULL, NULL, 0, NULL, 0);
	    if (SQL_SUCCEEDED(ret))
	    {
		*((SQLINTEGER *) DiagInfoPtr) = 1;
	    }
	    break;
	case SQL_DIAG_SQLSTATE:
	    rtnlen = 5;
	    ret = PGAPI_EnvError(Handle, RecNumber,
				 (UCHAR *)DiagInfoPtr,
				 NULL, NULL, 0, NULL, 0);
	    if (SQL_SUCCESS_WITH_INFO == ret)
		ret = SQL_SUCCESS;
	    break;
	case SQL_DIAG_RETURNCODE:	/* driver manager returns */
	    break;
	case SQL_DIAG_CURSOR_ROW_COUNT:
	case SQL_DIAG_ROW_COUNT:
	case SQL_DIAG_DYNAMIC_FUNCTION:
	case SQL_DIAG_DYNAMIC_FUNCTION_CODE:
	    /* options for statement type only */
	    break;
	}
	break;
    case SQL_HANDLE_DBC:
	conn = (ConnectionClass *) Handle;
	switch (DiagIdentifier)
	{
	case SQL_DIAG_CLASS_ORIGIN:
	case SQL_DIAG_SUBCLASS_ORIGIN:
	case SQL_DIAG_CONNECTION_NAME:
	    rtnlen = 0;
	    if (DiagInfoPtr && BufferLength > rtnlen)
	    {
		ret = SQL_SUCCESS;
		*((char *) DiagInfoPtr) = '\0';
	    } else
		ret = SQL_SUCCESS_WITH_INFO;
	    break;
	case SQL_DIAG_SERVER_NAME:
	    rtnlen = strlen(CC_get_DSN(conn));
	    if (DiagInfoPtr)
	    {
		strncpy_null((char *)DiagInfoPtr, CC_get_DSN(conn),
			     BufferLength);
		ret =
		    (BufferLength >
		     rtnlen ? SQL_SUCCESS : SQL_SUCCESS_WITH_INFO);
	    } else
		ret = SQL_SUCCESS_WITH_INFO;
	    break;
	case SQL_DIAG_MESSAGE_TEXT:
	    ret = PGAPI_ConnectError(Handle, RecNumber,
				     NULL, NULL, (UCHAR *)DiagInfoPtr,
				     BufferLength, StringLengthPtr, 0);
	    break;
	case SQL_DIAG_NATIVE:
	    rtnctype = SQL_C_LONG;
	    ret = PGAPI_ConnectError(Handle, RecNumber,
				     NULL, (SQLINTEGER *) DiagInfoPtr,
				     NULL, 0, NULL, 0);
	    break;
	case SQL_DIAG_NUMBER:
	    rtnctype = SQL_C_LONG;
	    ret = PGAPI_ConnectError(Handle, RecNumber,
				     NULL, NULL, NULL, 0, NULL, 0);
	    if (SQL_SUCCEEDED(ret))
	    {
		*((SQLINTEGER *) DiagInfoPtr) = 1;
	    }
	    break;
	case SQL_DIAG_SQLSTATE:
	    rtnlen = 5;
	    ret = PGAPI_ConnectError(Handle, RecNumber,
				     (UCHAR *)DiagInfoPtr, NULL, NULL,
				     0, NULL, 0);
	    if (SQL_SUCCESS_WITH_INFO == ret)
		ret = SQL_SUCCESS;
	    break;
	case SQL_DIAG_RETURNCODE:	/* driver manager returns */
	    break;
	case SQL_DIAG_CURSOR_ROW_COUNT:
	case SQL_DIAG_ROW_COUNT:
	case SQL_DIAG_DYNAMIC_FUNCTION:
	case SQL_DIAG_DYNAMIC_FUNCTION_CODE:
	    /* options for statement type only */
	    break;
	}
	break;
    case SQL_HANDLE_STMT:
	conn =
	    (ConnectionClass *)
	    SC_get_conn(((StatementClass *) Handle));
	switch (DiagIdentifier)
	{
	case SQL_DIAG_CLASS_ORIGIN:
	case SQL_DIAG_SUBCLASS_ORIGIN:
	case SQL_DIAG_CONNECTION_NAME:
	    rtnlen = 0;
	    if (DiagInfoPtr && BufferLength > rtnlen)
	    {
		ret = SQL_SUCCESS;
		*((char *) DiagInfoPtr) = '\0';
	    } else
		ret = SQL_SUCCESS_WITH_INFO;
	    break;
	case SQL_DIAG_SERVER_NAME:
	    rtnlen = strlen(CC_get_DSN(conn));
	    if (DiagInfoPtr)
	    {
		strncpy_null((char *) DiagInfoPtr, CC_get_DSN(conn),
			     BufferLength);
		ret =
		    (BufferLength >
		     rtnlen ? SQL_SUCCESS : SQL_SUCCESS_WITH_INFO);
	    } else
		ret = SQL_SUCCESS_WITH_INFO;
	    break;
	case SQL_DIAG_MESSAGE_TEXT:
	    ret = PGAPI_StmtError(Handle, RecNumber,
				  NULL, NULL, (UCHAR *)DiagInfoPtr,
				  BufferLength, StringLengthPtr, 0);
	    break;
	case SQL_DIAG_NATIVE:
	    rtnctype = SQL_C_LONG;
	    ret = PGAPI_StmtError(Handle, RecNumber,
				  NULL, (SQLINTEGER *) DiagInfoPtr,
				  NULL, 0, NULL, 0);
	    break;
	case SQL_DIAG_NUMBER:
	    rtnctype = SQL_C_LONG;
	    *((SQLINTEGER *) DiagInfoPtr) = 0;
	    ret = SQL_NO_DATA_FOUND;
	    stmt = (StatementClass *) Handle;
	    rtn = PGAPI_StmtError(Handle, -1, NULL,
				  NULL, NULL, 0, &pcbErrm, 0);
	    switch (rtn)
	    {
	    case SQL_SUCCESS:
	    case SQL_SUCCESS_WITH_INFO:
		ret = SQL_SUCCESS;
		if (pcbErrm > 0 && stmt->pgerror)

		    *((SQLINTEGER *) DiagInfoPtr) =
			(pcbErrm - 1) / stmt->pgerror->recsize + 1;
		break;
	    default:
		break;
	    }
	    break;
	case SQL_DIAG_SQLSTATE:
	    rtnlen = 5;
	    ret = PGAPI_StmtError(Handle, RecNumber,
				 (UCHAR *)DiagInfoPtr, NULL, NULL, 0, NULL, 0);
	    if (SQL_SUCCESS_WITH_INFO == ret)
		ret = SQL_SUCCESS;
	    break;
	case SQL_DIAG_CURSOR_ROW_COUNT:
	    rtnctype = SQL_C_LONG;
	    stmt = (StatementClass *) Handle;
	    rc = -1;
	    if (stmt->status == STMT_FINISHED)
	    {
		QResultClass *res = SC_get_Curres(stmt);

		/*if (!res)
		   return SQL_ERROR; */
		if (stmt->proc_return > 0)
		    rc = 0;
		else if (res && QR_NumResultCols(res) > 0)
		    rc = QR_get_num_total_tuples(res) - res->dl_count;
	    }
	    *((SQLLEN *) DiagInfoPtr) = rc;
	    inolog("rc=%d\n", rc);
	    ret = SQL_SUCCESS;
	    break;
	case SQL_DIAG_ROW_COUNT:
	    rtnctype = SQL_C_LONG;
	    stmt = (StatementClass *) Handle;
	    *((SQLLEN *) DiagInfoPtr) = stmt->diag_row_count;
	    ret = SQL_SUCCESS;
	    break;
	case SQL_DIAG_ROW_NUMBER:
	    rtnctype = SQL_C_LONG;
	    *((SQLLEN *) DiagInfoPtr) = SQL_ROW_NUMBER_UNKNOWN;
	    ret = SQL_SUCCESS;
	    break;
	case SQL_DIAG_COLUMN_NUMBER:
	    rtnctype = SQL_C_LONG;
	    *((SQLINTEGER *) DiagInfoPtr) = SQL_COLUMN_NUMBER_UNKNOWN;
	    ret = SQL_SUCCESS;
	    break;
	case SQL_DIAG_RETURNCODE:	/* driver manager returns */
	    break;
	}
	break;
    case SQL_HANDLE_DESC:
	conn = DC_get_conn(((DescriptorClass *) Handle));
	switch (DiagIdentifier)
	{
	case SQL_DIAG_CLASS_ORIGIN:
	case SQL_DIAG_SUBCLASS_ORIGIN:
	case SQL_DIAG_CONNECTION_NAME:
	    rtnlen = 0;
	    if (DiagInfoPtr && BufferLength > rtnlen)
	    {
		ret = SQL_SUCCESS;
		*((char *) DiagInfoPtr) = '\0';
	    } else
		ret = SQL_SUCCESS_WITH_INFO;
	    break;
	case SQL_DIAG_SERVER_NAME:
	    rtnlen = strlen(CC_get_DSN(conn));
	    if (DiagInfoPtr)
	    {
		strncpy_null((char *)DiagInfoPtr, CC_get_DSN(conn),
			     BufferLength);
		ret =
		    (BufferLength >
		     rtnlen ? SQL_SUCCESS : SQL_SUCCESS_WITH_INFO);
	    } else
		ret = SQL_SUCCESS_WITH_INFO;
	    break;
	case SQL_DIAG_MESSAGE_TEXT:
	case SQL_DIAG_NATIVE:
	case SQL_DIAG_NUMBER:
	    break;
	case SQL_DIAG_SQLSTATE:
	    rtnlen = 5;
	    ret = PGAPI_DescError(Handle, RecNumber,
				  (UCHAR *)DiagInfoPtr, NULL, NULL, 0, NULL, 0);
	    if (SQL_SUCCESS_WITH_INFO == ret)
		ret = SQL_SUCCESS;
	    break;
	case SQL_DIAG_RETURNCODE:	/* driver manager returns */
	    break;
	case SQL_DIAG_CURSOR_ROW_COUNT:
	case SQL_DIAG_ROW_COUNT:
	case SQL_DIAG_DYNAMIC_FUNCTION:
	case SQL_DIAG_DYNAMIC_FUNCTION_CODE:
	    rtnctype = SQL_C_LONG;
	    /* options for statement type only */
	    break;
	}
	break;
    default:
	ret = SQL_ERROR;
    }
    if (SQL_C_LONG == rtnctype)
    {
	if (SQL_SUCCESS_WITH_INFO == ret)
	    ret = SQL_SUCCESS;
	if (StringLengthPtr)
	    *StringLengthPtr = sizeof(SQLINTEGER);
    } else if (rtnlen >= 0)
    {
	if (rtnlen >= BufferLength)
	{
	    if (SQL_SUCCESS == ret)
		ret = SQL_SUCCESS_WITH_INFO;
	    if (BufferLength > 0)
		((char *) DiagInfoPtr)[BufferLength - 1] = '\0';
	}
	if (StringLengthPtr)
	    *StringLengthPtr = (SQLSMALLINT) rtnlen;
    }
    mylog("%s exiting %d\n", func, ret);
    return ret;
}

/*	SQLGetConnectOption -> SQLGetconnectAttr */
RETCODE SQL_API
PGAPI_GetConnectAttr(HDBC ConnectionHandle,
		     SQLINTEGER Attribute, PTR Value,
		     SQLINTEGER BufferLength, SQLINTEGER * StringLength)
{
    ConnectionClass *conn = (ConnectionClass *) ConnectionHandle;
    RETCODE ret = SQL_SUCCESS;
    SQLINTEGER len = 4;

    mylog("PGAPI_GetConnectAttr %d\n", Attribute);
    switch (Attribute)
    {
    case SQL_ATTR_ASYNC_ENABLE:
	*((SQLINTEGER *) Value) = SQL_ASYNC_ENABLE_OFF;
	break;
    case SQL_ATTR_AUTO_IPD:
	*((SQLINTEGER *) Value) = SQL_FALSE;
	break;
    case SQL_ATTR_CONNECTION_DEAD:
	*((SQLUINTEGER *) Value) = (conn->status == CONN_NOT_CONNECTED
				    || conn->status == CONN_DOWN);
	break;
    case SQL_ATTR_CONNECTION_TIMEOUT:
	*((SQLUINTEGER *) Value) = 0;
	break;
    case SQL_ATTR_METADATA_ID:
	*((SQLUINTEGER *) Value) = conn->stmtOptions.metadata_id;
	break;
    default:
	ret =
	    PGAPI_GetConnectOption(ConnectionHandle, (UWORD) Attribute,
				   Value, &len, BufferLength);
    }
    if (StringLength)
	*StringLength = len;
    return ret;
}

static SQLHDESC
descHandleFromStatementHandle(HSTMT StatementHandle,
			      SQLINTEGER descType)
{
    StatementClass *stmt = (StatementClass *) StatementHandle;

    switch (descType)
    {
    case SQL_ATTR_APP_ROW_DESC:	/* 10010 */
	return (HSTMT) stmt->ard;
    case SQL_ATTR_APP_PARAM_DESC:	/* 10011 */
	return (HSTMT) stmt->apd;
    case SQL_ATTR_IMP_ROW_DESC:	/* 10012 */
	return (HSTMT) stmt->ird;
    case SQL_ATTR_IMP_PARAM_DESC:	/* 10013 */
	return (HSTMT) stmt->ipd;
    }
    return (HSTMT) 0;
}

static void column_bindings_set(ARDFields * opts, int cols, BOOL maxset)
{
    int i;

    if (cols == opts->allocated)
	return;
    if (cols > opts->allocated)
    {
	extend_column_bindings(opts, cols);
	return;
    }
    if (maxset)
	return;

    for (i = opts->allocated; i > cols; i--)
	reset_a_column_binding(opts, i);
    opts->allocated = cols;
    if (0 == cols)
    {
	free(opts->bindings);
	opts->bindings = NULL;
    }
}

/*	SQLGetStmtOption -> SQLGetStmtAttr */
RETCODE SQL_API
PGAPI_GetStmtAttr(HSTMT StatementHandle,
		  SQLINTEGER Attribute, PTR Value,
		  SQLINTEGER BufferLength, SQLINTEGER * StringLength)
{
    CSTR func = "PGAPI_GetStmtAttr";
    StatementClass *stmt = (StatementClass *) StatementHandle;
    RETCODE ret = SQL_SUCCESS;
    SQLINTEGER len = 0;

    mylog("%s Handle=%p %d\n", func, StatementHandle, Attribute);
    switch (Attribute)
    {
    case SQL_ATTR_FETCH_BOOKMARK_PTR:	/* 16 */
	*((void **) Value) = stmt->options.bookmark_ptr;
	len = sizeof(SQLPOINTER);
	break;
    case SQL_ATTR_PARAM_BIND_OFFSET_PTR:	/* 17 */
	*((SQLULEN **) Value) =
	    (SQLULEN *) SC_get_APDF(stmt)->param_offset_ptr;
	len = sizeof(SQLPOINTER);
	break;
    case SQL_ATTR_PARAM_BIND_TYPE:	/* 18 */
	*((SQLUINTEGER *) Value) = SC_get_APDF(stmt)->param_bind_type;
	len = sizeof(SQLUINTEGER);
	break;
    case SQL_ATTR_PARAM_OPERATION_PTR:	/* 19 */
	*((SQLUSMALLINT **) Value) =
	    SC_get_APDF(stmt)->param_operation_ptr;
	len = sizeof(SQLPOINTER);
	break;
    case SQL_ATTR_PARAM_STATUS_PTR:	/* 20 */
	*((SQLUSMALLINT **) Value) =
	    SC_get_IPDF(stmt)->param_status_ptr;
	len = sizeof(SQLPOINTER);
	break;
    case SQL_ATTR_PARAMS_PROCESSED_PTR:	/* 21 */
	*((SQLUINTEGER **) Value) =
	    (SQLUINTEGER *) SC_get_IPDF(stmt)->param_processed_ptr;
	len = sizeof(SQLPOINTER);
	break;
    case SQL_ATTR_PARAMSET_SIZE:	/* 22 */
	*((SQLUINTEGER *) Value) =
	    (SQLUINTEGER) SC_get_APDF(stmt)->paramset_size;
	len = sizeof(SQLUINTEGER);
	break;
    case SQL_ATTR_ROW_BIND_OFFSET_PTR:	/* 23 */
	*((SQLULEN **) Value) =
	    (SQLULEN *) SC_get_ARDF(stmt)->row_offset_ptr;
	len = 4;
	break;
    case SQL_ATTR_ROW_OPERATION_PTR:	/* 24 */
	*((SQLUSMALLINT **) Value) =
	    SC_get_ARDF(stmt)->row_operation_ptr;
	len = 4;
	break;
    case SQL_ATTR_ROW_STATUS_PTR:	/* 25 */
	*((SQLUSMALLINT **) Value) = SC_get_IRDF(stmt)->rowStatusArray;
	len = 4;
	break;
    case SQL_ATTR_ROWS_FETCHED_PTR:	/* 26 */
	*((SQLULEN **) Value) =
	    (SQLULEN *) SC_get_IRDF(stmt)->rowsFetched;
	len = 4;
	break;
    case SQL_ATTR_ROW_ARRAY_SIZE:	/* 27 */
	*((SQLULEN *) Value) = SC_get_ARDF(stmt)->size_of_rowset;
	len = 4;
	break;
    case SQL_ATTR_APP_ROW_DESC:	/* 10010 */
    case SQL_ATTR_APP_PARAM_DESC:	/* 10011 */
    case SQL_ATTR_IMP_ROW_DESC:	/* 10012 */
    case SQL_ATTR_IMP_PARAM_DESC:	/* 10013 */
	len = 4;
	*((HSTMT *) Value) =
	    descHandleFromStatementHandle(StatementHandle, Attribute);
	break;

    case SQL_ATTR_CURSOR_SCROLLABLE:	/* -1 */
	len = 4;
	if (SQL_CURSOR_FORWARD_ONLY == stmt->options.cursor_type)
	    *((SQLUINTEGER *) Value) = SQL_NONSCROLLABLE;
	else
	    *((SQLUINTEGER *) Value) = SQL_SCROLLABLE;
	break;
    case SQL_ATTR_CURSOR_SENSITIVITY:	/* -2 */
	len = 4;
	if (SQL_CONCUR_READ_ONLY == stmt->options.scroll_concurrency)
	    *((SQLUINTEGER *) Value) = SQL_INSENSITIVE;
	else
	    *((SQLUINTEGER *) Value) = SQL_UNSPECIFIED;
	break;
    case SQL_ATTR_METADATA_ID:	/* 10014 */
	*((SQLUINTEGER *) Value) = stmt->options.metadata_id;
	break;
    case SQL_ATTR_ENABLE_AUTO_IPD:	/* 15 */
	*((SQLUINTEGER *) Value) = SQL_FALSE;
	break;
    case SQL_ATTR_AUTO_IPD:	/* 10001 */
	/* case SQL_ATTR_ROW_BIND_TYPE: ** == SQL_BIND_TYPE(ODBC2.0) */
	SC_set_error(stmt, DESC_INVALID_OPTION_IDENTIFIER,
		     "Unsupported statement option (Get)", func);
	return SQL_ERROR;
    default:
	ret =
	    PGAPI_GetStmtOption(StatementHandle,
				(SQLSMALLINT) Attribute, Value, &len,
				BufferLength);
    }
    if (ret == SQL_SUCCESS && StringLength)
	*StringLength = len;
    return ret;
}

/*	SQLSetConnectOption -> SQLSetConnectAttr */
RETCODE SQL_API
PGAPI_SetConnectAttr(HDBC ConnectionHandle,
		     SQLINTEGER Attribute, PTR Value,
		     SQLINTEGER StringLength)
{
    CSTR func = "PGAPI_SetConnectAttr";
    ConnectionClass *conn = (ConnectionClass *) ConnectionHandle;
    RETCODE ret = SQL_SUCCESS;
    BOOL unsupported = FALSE;

    mylog("%s for %p: %d %p\n", func, ConnectionHandle, Attribute,
	  Value);
    switch (Attribute)
    {
    case SQL_ATTR_METADATA_ID:
	conn->stmtOptions.metadata_id = CAST_UPTR(SQLUINTEGER, Value);
	break;
    case SQL_ATTR_ANSI_APP:
	if (SQL_AA_FALSE != CAST_PTR(SQLINTEGER, Value))
	{
	    mylog("the application is ansi\n");
	    if (CC_is_in_unicode_driver(conn))	/* the driver is unicode */
		CC_set_in_ansi_app(conn);	/* but the app is ansi */
	} else
	{
	    mylog("the application is unicode\n");
	}
	/*return SQL_ERROR; */
	return SQL_SUCCESS;
    case SQL_ATTR_ENLIST_IN_DTC:
#ifdef	WIN32
#ifdef	_HANDLE_ENLIST_IN_DTC_
	mylog("SQL_ATTR_ENLIST_IN_DTC %p request received\n", Value);
	if (conn->connInfo.xa_opt != 0)
	    return CALL_EnlistInDtc(conn, Value, conn->connInfo.xa_opt);
#endif				/* _HANDLE_ENLIST_IN_DTC_ */
#endif				/* WIN32 */
	unsupported = TRUE;
	break;
    case SQL_ATTR_AUTO_IPD:
	if (SQL_FALSE != Value)
	    unsupported = TRUE;
	break;
    case SQL_ATTR_ASYNC_ENABLE:
    case SQL_ATTR_CONNECTION_DEAD:
    case SQL_ATTR_CONNECTION_TIMEOUT:
	unsupported = TRUE;
	break;
    default:
	ret =
	    PGAPI_SetConnectOption(ConnectionHandle,
				   (SQLUSMALLINT) Attribute,
				   (SQLLEN) Value);
    }
    if (unsupported)
    {
	char msg[64];
	snprintf(msg, sizeof(msg),
		 "Couldn't set unsupported connect attribute "
		 FORMAT_LPTR, (LONG_PTR) Value);
	CC_set_error(conn, CONN_OPTION_NOT_FOR_THE_DRIVER, msg, func);
	return SQL_ERROR;
    }
    return ret;
}

/*	new function */
RETCODE SQL_API
PGAPI_GetDescField(SQLHDESC DescriptorHandle,
		   SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
		   PTR Value, SQLINTEGER BufferLength,
		   SQLINTEGER * StringLength)
{
    CSTR func = "PGAPI_GetDescField";
    RETCODE ret = SQL_SUCCESS;
    DescriptorClass *desc = (DescriptorClass *) DescriptorHandle;

    mylog("%s h=%p rec=%d field=%d blen=%d\n", func, DescriptorHandle,
	  RecNumber, FieldIdentifier, BufferLength);

    mylog("PGAPI_GetDescField not yet implemented\n");
    DC_set_error(desc, DESC_INTERNAL_ERROR, "Error not implemented");
    return SQL_ERROR;
}

/*	new function */
RETCODE SQL_API
PGAPI_SetDescField(SQLHDESC DescriptorHandle,
		   SQLSMALLINT RecNumber, SQLSMALLINT FieldIdentifier,
		   PTR Value, SQLINTEGER BufferLength)
{
    CSTR func = "PGAPI_SetDescField";
    RETCODE ret = SQL_SUCCESS;
    DescriptorClass *desc = (DescriptorClass *) DescriptorHandle;

    mylog("PGAPI_SetDescField not yet implemented");
    DC_set_error(desc, DESC_INTERNAL_ERROR, "Error not implemented");
    return SQL_ERROR;
}

/*	SQLSet(Param/Scroll/Stmt)Option -> SQLSetStmtAttr */
RETCODE SQL_API
PGAPI_SetStmtAttr(HSTMT StatementHandle,
		  SQLINTEGER Attribute, PTR Value,
		  SQLINTEGER StringLength)
{
    RETCODE ret = SQL_SUCCESS;
    CSTR func = "PGAPI_SetStmtAttr";
    StatementClass *stmt = (StatementClass *) StatementHandle;

    mylog("%s Handle=%p %d,%u(%p)\n", func, StatementHandle, Attribute,
	  Value, Value);
    switch (Attribute)
    {
    case SQL_ATTR_ENABLE_AUTO_IPD:	/* 15 */
	if (SQL_FALSE == Value)
	    break;
    case SQL_ATTR_CURSOR_SCROLLABLE:	/* -1 */
    case SQL_ATTR_CURSOR_SENSITIVITY:	/* -2 */
    case SQL_ATTR_AUTO_IPD:	/* 10001 */
	SC_set_error(stmt, DESC_OPTION_NOT_FOR_THE_DRIVER,
		     "Unsupported statement option (Set)", func);
	return SQL_ERROR;
	/* case SQL_ATTR_ROW_BIND_TYPE: ** == SQL_BIND_TYPE(ODBC2.0) */
    case SQL_ATTR_IMP_ROW_DESC:	/* 10012 (read-only) */
    case SQL_ATTR_IMP_PARAM_DESC:	/* 10013 (read-only) */

	/*
	 * case SQL_ATTR_PREDICATE_PTR: case
	 * SQL_ATTR_PREDICATE_OCTET_LENGTH_PTR:
	 */
	SC_set_error(stmt, DESC_INVALID_OPTION_IDENTIFIER,
		     "Unsupported statement option (Set)", func);
	return SQL_ERROR;

    case SQL_ATTR_METADATA_ID:	/* 10014 */
	stmt->options.metadata_id = CAST_UPTR(SQLUINTEGER, Value);
	break;
    case SQL_ATTR_APP_ROW_DESC:	/* 10010 */
	if (SQL_NULL_HDESC == Value)
	{
	    stmt->ard = &(stmt->ardi);
	} else
	{
	    stmt->ard = (ARDClass *) Value;
	    inolog("set ard=%p\n", stmt->ard);
	}
	break;
    case SQL_ATTR_APP_PARAM_DESC:	/* 10011 */
	if (SQL_NULL_HDESC == Value)
	{
	    stmt->apd = &(stmt->apdi);
	} else
	{
	    stmt->apd = (APDClass *) Value;
	}
	break;
    case SQL_ATTR_FETCH_BOOKMARK_PTR:	/* 16 */
	stmt->options.bookmark_ptr = Value;
	break;
    case SQL_ATTR_PARAM_BIND_OFFSET_PTR:	/* 17 */
	SC_get_APDF(stmt)->param_offset_ptr = (SQLULEN *) Value;
	break;
    case SQL_ATTR_PARAM_BIND_TYPE:	/* 18 */
	SC_get_APDF(stmt)->param_bind_type =
	    CAST_UPTR(SQLUINTEGER, Value);
	break;
    case SQL_ATTR_PARAM_OPERATION_PTR:	/* 19 */
	SC_get_APDF(stmt)->param_operation_ptr = (SQLUSMALLINT*)Value;
	break;
    case SQL_ATTR_PARAM_STATUS_PTR:	/* 20 */
	SC_get_IPDF(stmt)->param_status_ptr = (SQLUSMALLINT *) Value;
	break;
    case SQL_ATTR_PARAMS_PROCESSED_PTR:	/* 21 */
	SC_get_IPDF(stmt)->param_processed_ptr = (SQLUINTEGER *) Value;
	break;
    case SQL_ATTR_PARAMSET_SIZE:	/* 22 */
	SC_get_APDF(stmt)->paramset_size =
	    CAST_UPTR(SQLUINTEGER, Value);
	break;
    case SQL_ATTR_ROW_BIND_OFFSET_PTR:	/* 23 */
	SC_get_ARDF(stmt)->row_offset_ptr = (SQLULEN *) Value;
	break;
    case SQL_ATTR_ROW_OPERATION_PTR:	/* 24 */
	SC_get_ARDF(stmt)->row_operation_ptr = (SQLUSMALLINT*)Value;
	break;
    case SQL_ATTR_ROW_STATUS_PTR:	/* 25 */
	SC_get_IRDF(stmt)->rowStatusArray = (SQLUSMALLINT *) Value;
	break;
    case SQL_ATTR_ROWS_FETCHED_PTR:	/* 26 */
	SC_get_IRDF(stmt)->rowsFetched = (SQLULEN *) Value;
	break;
    case SQL_ATTR_ROW_ARRAY_SIZE:	/* 27 */
	SC_get_ARDF(stmt)->size_of_rowset = (SQLULEN) Value;
	break;
    default:
	return PGAPI_SetStmtOption(StatementHandle,
				   (SQLUSMALLINT) Attribute,
				   (SQLULEN) Value);
    }
    return ret;
}

#define	CALC_BOOKMARK_ADDR(book, offset, bind_size, index) \
	(book->buffer + offset + \
	(bind_size > 0 ? bind_size : (SQL_C_VARBOOKMARK == book->returntype ? book->buflen : sizeof(UInt4))) * index)


RETCODE SQL_API
PGAPI_BulkOperations(HSTMT hstmt, SQLSMALLINT operationX)
{
    CSTR func = "PGAPI_BulkOperations";
    SC_set_error((StatementClass *)hstmt, DESC_INTERNAL_ERROR, 
	"Bulk operations are not yet supported.", func);
    return SQL_ERROR;
}
