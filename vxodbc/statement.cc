/*
 * Description:	This module contains functions related to creating
 *		and manipulating a statement.
 */

#ifndef	_WIN32_WINNT
#define	_WIN32_WINNT	0x0400
#endif				/* _WIN32_WINNT */

#include "statement.h"

#include "bind.h"
#include "connection.h"
#include "multibyte.h"
#include "qresult.h"
#include "convert.h"
#include "environ.h"

#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include "pgapifunc.h"


#define PRN_NULLCHECK

/*	Map sql commands to statement types */
static struct {
    int type;
    char *s;
} Statement_Type[] =
{
    {
    STMT_TYPE_SELECT, "SELECT"}
    ,
    {
    STMT_TYPE_INSERT, "INSERT"}
    ,
    {
    STMT_TYPE_UPDATE, "UPDATE"}
    ,
    {
    STMT_TYPE_DELETE, "DELETE"}
    ,
    {
    STMT_TYPE_PROCCALL, "{"}
    ,
    {
    STMT_TYPE_SET, "SET"}
    ,
    {
    STMT_TYPE_RESET, "RESET"}
    ,
    {
    STMT_TYPE_CREATE, "CREATE"}
    ,
    {
    STMT_TYPE_DECLARE, "DECLARE"}
    ,
    {
    STMT_TYPE_FETCH, "FETCH"}
    ,
    {
    STMT_TYPE_MOVE, "MOVE"}
    ,
    {
    STMT_TYPE_CLOSE, "CLOSE"}
    ,
    {
    STMT_TYPE_PREPARE, "PREPARE"}
    ,
    {
    STMT_TYPE_EXECUTE, "EXECUTE"}
    ,
    {
    STMT_TYPE_DEALLOCATE, "DEALLOCATE"}
    ,
    {
    STMT_TYPE_DROP, "DROP"}
    ,
    {
    STMT_TYPE_START, "BEGIN"}
    ,
    {
    STMT_TYPE_START, "START"}
    ,
    {
    STMT_TYPE_TRANSACTION, "SAVEPOINT"}
    ,
    {
    STMT_TYPE_TRANSACTION, "RELEASE"}
    ,
    {
    STMT_TYPE_TRANSACTION, "COMMIT"}
    ,
    {
    STMT_TYPE_TRANSACTION, "END"}
    ,
    {
    STMT_TYPE_TRANSACTION, "ROLLBACK"}
    ,
    {
    STMT_TYPE_TRANSACTION, "ABORT"}
    ,
    {
    STMT_TYPE_LOCK, "LOCK"}
    ,
    {
    STMT_TYPE_ALTER, "ALTER"}
    ,
    {
    STMT_TYPE_GRANT, "GRANT"}
    ,
    {
    STMT_TYPE_REVOKE, "REVOKE"}
    ,
    {
    STMT_TYPE_COPY, "COPY"}
    ,
    {
    STMT_TYPE_ANALYZE, "ANALYZE"}
    ,
    {
    STMT_TYPE_NOTIFY, "NOTIFY"}
    ,
    {
    STMT_TYPE_EXPLAIN, "EXPLAIN"}
    ,
    {
    STMT_TYPE_SPECIAL, "VACUUM"}
    ,
    {
    STMT_TYPE_SPECIAL, "REINDEX"}
    ,
    {
    STMT_TYPE_SPECIAL, "CLUSTER"}
    ,
    {
    STMT_TYPE_SPECIAL, "CHECKPOINT"}
    ,
    {
    0, NULL}
};


void SC_set_Result(StatementClass *s, QResultClass *q)
{
    if (q != s->result)
    {
	mylog("SC_set_Result(%x, %x)\n", s, q);
	QR_Destructor(s->result);
	s->result = s->curres = q;
    }
}


RETCODE SQL_API PGAPI_AllocStmt(HDBC hdbc, HSTMT FAR * phstmt)
{
    CSTR func = "PGAPI_AllocStmt";
    ConnectionClass *conn = (ConnectionClass *) hdbc;
    StatementClass *stmt;
    ARDFields *ardopts;
    BindInfoClass *bookmark;

    mylog("%s: entering...\n", func);

    if (!conn)
    {
	CC_log_error(func, "", NULL);
	return SQL_INVALID_HANDLE;
    }

    stmt = SC_Constructor(conn);

    mylog("**** PGAPI_AllocStmt: hdbc = %p, stmt = %p\n", hdbc, stmt);

    if (!stmt)
    {
	CC_set_error(conn, CONN_STMT_ALLOC_ERROR,
		     "No more memory to allocate a further SQL-statement",
		     func);
	*phstmt = SQL_NULL_HSTMT;
	return SQL_ERROR;
    }

    if (!CC_add_statement(conn, stmt))
    {
	CC_set_error(conn, CONN_STMT_ALLOC_ERROR,
		     "Maximum number of statements exceeded.", func);
	SC_Destructor(stmt);
	*phstmt = SQL_NULL_HSTMT;
	return SQL_ERROR;
    }

    *phstmt = (HSTMT) stmt;

    /* Copy default statement options based from Connection options */
    stmt->options = stmt->options_orig = conn->stmtOptions;
    stmt->ardi.ardopts = conn->ardOptions;
    ardopts = SC_get_ARDF(stmt);
    bookmark = ARD_AllocBookmark(ardopts);

    stmt->stmt_size_limit = CC_get_max_query_len(conn);
    /* Save the handle for later */
    stmt->phstmt = phstmt;

    return SQL_SUCCESS;
}


RETCODE SQL_API PGAPI_FreeStmt(HSTMT hstmt, SQLUSMALLINT fOption)
{
    CSTR func = "PGAPI_FreeStmt";
    StatementClass *stmt = (StatementClass *) hstmt;

    mylog("hstmt=%p, fOption=%hi\n", hstmt, fOption);

    if (!stmt)
    {
	SC_log_error(func, "", NULL);
	return SQL_INVALID_HANDLE;
    }
    SC_clear_error(stmt);

    if (fOption == SQL_DROP)
    {
	ConnectionClass *conn = stmt->hdbc;

	/* Remove the statement from the connection's statement list */
	if (conn)
	{
	    QResultClass *res;

	    if (!CC_remove_statement(conn, stmt))
	    {
		SC_set_error(stmt, STMT_SEQUENCE_ERROR,
			     "Statement is currently executing a transaction.",
			     func);
		return SQL_ERROR;	/* stmt may be executing a
					 * transaction */
	    }

	    /* Free any cursors and discard any result info */
	    res = SC_get_Result(stmt);
	    QR_Destructor(res);
	    SC_init_Result(stmt);
	}

	if (stmt->execute_delegate)
	{
	    PGAPI_FreeStmt(stmt->execute_delegate, SQL_DROP);
	    stmt->execute_delegate = NULL;
	}
	if (stmt->execute_parent)
	    stmt->execute_parent->execute_delegate = NULL;
	/* Destroy the statement and free any results, cursors, etc. */
	SC_Destructor(stmt);
    } else if (fOption == SQL_UNBIND)
	SC_unbind_cols(stmt);
    else if (fOption == SQL_CLOSE)
    {
	/*
	 * this should discard all the results, but leave the statement
	 * itself in place (it can be executed again)
	 */
	stmt->transition_status = 0;
	if (stmt->execute_delegate)
	{
	    PGAPI_FreeStmt(stmt->execute_delegate, SQL_DROP);
	    stmt->execute_delegate = NULL;
	}
	if (!SC_recycle_statement(stmt))
	{
	    return SQL_ERROR;
	}
    } 
    else
    {
	SC_set_error(stmt, STMT_OPTION_OUT_OF_RANGE_ERROR,
		     "Invalid option passed to PGAPI_FreeStmt.", func);
	return SQL_ERROR;
    }

    return SQL_SUCCESS;
}


/*
 * StatementClass implementation
 */
void InitializeStatementOptions(StatementOptions * opt)
{
    memset(opt, 0, sizeof(StatementOptions));
    opt->maxRows = 0;		/* driver returns all rows */
    opt->maxLength = 0;		/* driver returns all data for char/binary */
    opt->keyset_size = 0;	/* fully keyset driven is the default */
    opt->scroll_concurrency = SQL_CONCUR_READ_ONLY;
    opt->cursor_type = SQL_CURSOR_FORWARD_ONLY;
    opt->retrieve_data = SQL_RD_ON;
    opt->use_bookmarks = SQL_UB_OFF;
    opt->metadata_id = SQL_FALSE;
}

static void SC_clear_parse_status(StatementClass * self,
				  ConnectionClass * conn)
{
    self->parse_status = STMT_PARSE_NONE;
    if (PG_VERSION_LT(conn, 7.2))
    {
	SC_set_checked_hasoids(self, TRUE);
	self->num_key_fields = PG_NUM_NORMAL_KEYS;
    }
}

static void SC_init_discard_output_params(StatementClass * self)
{
    ConnectionClass *conn = SC_get_conn(self);

    if (!conn)
	return;
    self->discard_output_params = 0;
    if (!conn->connInfo.use_server_side_prepare)
	self->discard_output_params = 1;
}

static void SC_init_parse_method(StatementClass * self)
{
    ConnectionClass *conn = SC_get_conn(self);

    self->parse_method = 0;
    if (!conn)
	return;
    if (self->catalog_result)
	return;
    if (self->multi_statement <= 0 && conn->connInfo.disallow_premature)
	SC_set_parse_tricky(self);
}

StatementClass *SC_Constructor(ConnectionClass * conn)
{
    StatementClass *rv;

    rv = (StatementClass *) malloc(sizeof(StatementClass));
    if (rv)
    {
	rv->hdbc = conn;
	rv->phstmt = NULL;
	rv->result = NULL;
	rv->curres = NULL;
	rv->catalog_result = FALSE;
	rv->prepare = NON_PREPARE_STATEMENT;
	rv->prepared = NOT_YET_PREPARED;
	rv->status = STMT_ALLOCATED;
	rv->internal = FALSE;
	rv->plan_name = NULL;
	rv->transition_status = 0;
	rv->multi_statement = -1;	/* unknown */
	rv->num_params = -1;	/* unknown */

	rv->__error_message = NULL;
	rv->__error_number = 0;
	rv->pgerror = NULL;

	rv->statement = NULL;
	rv->stmt_with_params = NULL;
	rv->load_statement = NULL;
	rv->execute_statement = NULL;
	rv->stmt_size_limit = -1;
	rv->statement_type = STMT_TYPE_UNKNOWN;

	rv->currTuple = -1;
	SC_set_rowset_start(rv, -1, FALSE);
	rv->current_col = -1;
	rv->bind_row = 0;
	rv->last_fetch_count = rv->last_fetch_count_include_ommitted =
	    0;
	rv->save_rowset_size = -1;

	rv->data_at_exec = -1;
	rv->current_exec_param = -1;
	rv->exec_start_row = -1;
	rv->exec_end_row = -1;
	rv->exec_current_row = -1;
	rv->put_data = FALSE;
	rv->ref_CC_error = FALSE;
	rv->lock_CC_for_rb = 0;
	rv->join_info = 0;
	SC_init_parse_method(rv);

	INIT_NAME(rv->cursor_name);

	/* Parse Stuff */
	rv->ti = NULL;
	rv->ntab = 0;
	rv->num_key_fields = -1;	/* unknown */
	SC_clear_parse_status(rv, conn);
	rv->proc_return = -1;
	SC_init_discard_output_params(rv);
	rv->cancel_info = 0;

	/* Clear Statement Options -- defaults will be set in AllocStmt */
	memset(&rv->options, 0, sizeof(StatementOptions));
	InitializeEmbeddedDescriptor((DescriptorClass *) & (rv->ardi),
				     rv, SQL_ATTR_APP_ROW_DESC);
	InitializeEmbeddedDescriptor((DescriptorClass *) & (rv->apdi),
				     rv, SQL_ATTR_APP_PARAM_DESC);
	InitializeEmbeddedDescriptor((DescriptorClass *) & (rv->irdi),
				     rv, SQL_ATTR_IMP_ROW_DESC);
	InitializeEmbeddedDescriptor((DescriptorClass *) & (rv->ipdi),
				     rv, SQL_ATTR_IMP_PARAM_DESC);

	rv->pre_executing = FALSE;
	rv->inaccurate_result = FALSE;
	rv->miscinfo = 0;
	rv->updatable = FALSE;
	rv->diag_row_count = 0;
	rv->stmt_time = 0;
	rv->execute_delegate = NULL;
	rv->execute_parent = NULL;
	rv->allocated_callbacks = 0;
	rv->num_callbacks = 0;
	rv->callbacks = NULL;
	GetDataInfoInitialize(SC_get_GDTI(rv));
	INIT_STMT_CS(rv);
    }
    return rv;
}

char SC_Destructor(StatementClass * self)
{
    CSTR func = "SC_Destrcutor";
    QResultClass *res = SC_get_Result(self);

    if (!self)
	return FALSE;
    mylog("self=%p, self->result=%p, self->hdbc=%p\n",
	  self, res, self->hdbc);
    SC_clear_error(self);
    if (STMT_EXECUTING == self->status)
    {
	SC_set_error(self, STMT_SEQUENCE_ERROR,
		     "Statement is currently executing a transaction.",
		     func);
	return FALSE;
    }

    if (res)
    {
	if (!self->hdbc)
	    res->conn = NULL;	/* prevent any dbase activity */

	QR_Destructor(res);
    }

    SC_initialize_stmts(self, TRUE);

    /* Free the parsed table information */
    SC_initialize_cols_info(self, FALSE, TRUE);

    NULL_THE_NAME(self->cursor_name);
    /* Free the parsed field information */
    DC_Destructor((DescriptorClass *) SC_get_ARDi(self));
    DC_Destructor((DescriptorClass *) SC_get_APDi(self));
    DC_Destructor((DescriptorClass *) SC_get_IRDi(self));
    DC_Destructor((DescriptorClass *) SC_get_IPDi(self));
    GDATA_unbind_cols(SC_get_GDTI(self), TRUE);

    if (self->__error_message)
	free(self->__error_message);
    if (self->pgerror)
	ER_Destructor(self->pgerror);
    cancelNeedDataState(self);
    if (self->callbacks)
	free(self->callbacks);

    DELETE_STMT_CS(self);
    free(self);

    mylog("done\n");
    return TRUE;
}


int statement_type(const char *statement)
{
    int i;

    /* ignore leading whitespace in query string */
    while (*statement
	   && (isspace((UCHAR) * statement) || *statement == '('))
	statement++;

    for (i = 0; Statement_Type[i].s; i++)
	if (!strnicmp
	    (statement, Statement_Type[i].s,
	     strlen(Statement_Type[i].s)))
	    return Statement_Type[i].type;

    return STMT_TYPE_OTHER;
}

void SC_set_planname(StatementClass * stmt, const char *plan_name)
{
    if (stmt->plan_name)
	free(stmt->plan_name);
    if (plan_name && plan_name[0])
	stmt->plan_name = strdup(plan_name);
    else
	stmt->plan_name = NULL;
}

void
SC_set_rowset_start(StatementClass * stmt, SQLLEN start,
		    BOOL valid_base)
{
    QResultClass *res = SC_get_Curres(stmt);
    SQLLEN incr = start - stmt->rowset_start;

    inolog("%p->SC_set_rowstart " FORMAT_LEN "->" FORMAT_LEN "(%s) ",
	   stmt, stmt->rowset_start, start,
	   valid_base ? "valid" : "unknown");
    if (res != NULL)
    {
	BOOL valid = QR_has_valid_base(res);
	inolog(":QR is %s",
	       QR_has_valid_base(res) ? "valid" : "unknown");

	if (valid)
	{
	    if (valid_base)
		QR_inc_rowstart_in_cache(res, incr);
	    else
		QR_set_no_valid_base(res);
	} else if (valid_base)
	{
	    QR_set_has_valid_base(res);
	    if (start < 0)
		QR_set_rowstart_in_cache(res, -1);
	    else
		QR_set_rowstart_in_cache(res, 0);
	}
	if (!QR_get_cursor(res))
	    res->key_base = start;
	inolog(":QR result=" FORMAT_LEN "(%s)",
	       QR_get_rowstart_in_cache(res),
	       QR_has_valid_base(res) ? "valid" : "unknown");
    }
    stmt->rowset_start = start;
    inolog(":stmt result=" FORMAT_LEN "\n", stmt->rowset_start);
}

void SC_inc_rowset_start(StatementClass * stmt, SQLLEN inc)
{
    SQLLEN start = stmt->rowset_start + inc;

    SC_set_rowset_start(stmt, start, TRUE);
}
int SC_set_current_col(StatementClass * stmt, int col)
{
    if (col == stmt->current_col)
	return col;
    if (col >= 0)
	reset_a_getdata_info(SC_get_GDTI(stmt), col + 1);
    stmt->current_col = col;

    return stmt->current_col;
}

// VX_CLEANUP: We don't support much in the way of prepared statements, this
// may be unused.  It sure is useless.
void SC_set_prepared(StatementClass * stmt, BOOL prepared)
{
    if (NOT_YET_PREPARED == prepared)
	SC_set_planname(stmt, NULL);
    stmt->prepared = prepared;
}

/*
 *	Initialize stmt_with_params, load_statement and execute_statement
 *		member pointer deallocating corresponding prepared plan.
 *	Also initialize statement member pointer if specified.
 */
RETCODE
SC_initialize_stmts(StatementClass * self, BOOL initializeOriginal)
{
    ConnectionClass *conn = SC_get_conn(self);

    if (self->lock_CC_for_rb > 0)
    {
	while (self->lock_CC_for_rb > 0)
	{
	    LEAVE_CONN_CS(conn);
	    self->lock_CC_for_rb--;
	}
    }
    if (initializeOriginal)
    {
	if (self->statement)
	{
	    free(self->statement);
	    self->statement = NULL;
	}
	if (self->execute_statement)
	{
	    free(self->execute_statement);
	    self->execute_statement = NULL;
	}
	self->prepare = NON_PREPARE_STATEMENT;
	SC_set_prepared(self, NOT_YET_PREPARED);
	self->statement_type = STMT_TYPE_UNKNOWN;	/* unknown */
	self->multi_statement = -1;	/* unknown */
	self->num_params = -1;	/* unknown */
	self->proc_return = -1;	/* unknown */
	self->join_info = 0;
	SC_init_parse_method(self);
	SC_init_discard_output_params(self);
    }
    if (self->stmt_with_params)
    {
	free(self->stmt_with_params);
	self->stmt_with_params = NULL;
    }
    if (self->load_statement)
    {
	free(self->load_statement);
	self->load_statement = NULL;
    }

    return 0;
}

BOOL SC_opencheck(StatementClass * self, const char *func)
{
    QResultClass *res;

    if (!self)
	return FALSE;
    if (self->status == STMT_EXECUTING)
    {
	SC_set_error(self, STMT_SEQUENCE_ERROR,
		     "Statement is currently executing a transaction.",
		     func);
	return TRUE;
    }
    /*
     * We can dispose the result of PREMATURE execution any time.
     */
    if (self->prepare && self->status == STMT_PREMATURE)
    {
	mylog
	    ("SC_opencheck: self->prepare && self->status == STMT_PREMATURE\n");
	return FALSE;
    }
    if (res = SC_get_Curres(self), NULL != res)
    {
	if (QR_command_maybe_successful(res) && res->backend_tuples)
	{
	    SC_set_error(self, STMT_SEQUENCE_ERROR,
			 "The cursor is open.", func);
	    return TRUE;
	}
    }

    return FALSE;
}

RETCODE SC_initialize_and_recycle(StatementClass * self)
{
    SC_initialize_stmts(self, TRUE);
    if (!SC_recycle_statement(self))
	return SQL_ERROR;

    return SQL_SUCCESS;
}

/*
 *	Called from SQLPrepare if STMT_PREMATURE, or
 *	from SQLExecute if STMT_FINISHED, or
 *	from SQLFreeStmt(SQL_CLOSE)
 */
char SC_recycle_statement(StatementClass * self)
{
    CSTR func = "SC_recycle_statement";
    ConnectionClass *conn;
    QResultClass *res;

    mylog("%s: self= %p\n", func, self);

    SC_clear_error(self);
    /* This would not happen */
    if (self->status == STMT_EXECUTING)
    {
	SC_set_error(self, STMT_SEQUENCE_ERROR,
		     "Statement is currently executing a transaction.",
		     func);
	return FALSE;
    }

    conn = SC_get_conn(self);
    switch (self->status)
    {
    case STMT_ALLOCATED:
	/* this statement does not need to be recycled */
	return TRUE;

    case STMT_READY:
	break;

    case STMT_PREMATURE:

	/*
	 * Premature execution of the statement might have caused the
	 * start of a transaction. If so, we have to rollback that
	 * transaction.
	 */
	// VX_CLEANUP: This was cleaned up.
	break;

    case STMT_FINISHED:
	break;

    default:
	SC_set_error(self, STMT_INTERNAL_ERROR,
		     "An internal error occured while recycling statements",
		     func);
	return FALSE;
    }

    switch (self->prepared)
    {
    case NOT_YET_PREPARED:
    case ONCE_DESCRIBED:
	/* Free the parsed table/field information */
	SC_initialize_cols_info(self, TRUE, TRUE);

	inolog("SC_clear_parse_status\n");
	SC_clear_parse_status(self, conn);
	break;
    }

    /* Free any cursors */
    if (res = SC_get_Result(self), res)
    {
	if (PREPARED_PERMANENTLY == self->prepared)
	    QR_close_result(res, FALSE);
	else
	{
	    QR_Destructor(res);
	    SC_init_Result(self);
	}
    }
    self->inaccurate_result = FALSE;
    self->miscinfo = 0;

    /*
     * Reset only parameters that have anything to do with results
     */
    self->status = STMT_READY;
    self->catalog_result = FALSE;	/* not very important */

    self->currTuple = -1;
    SC_set_rowset_start(self, -1, FALSE);
    SC_set_current_col(self, -1);
    self->bind_row = 0;
    inolog("%s statement=%p ommitted=0\n", func, self);
    self->last_fetch_count = self->last_fetch_count_include_ommitted =
	0;

    self->__error_message = NULL;
    self->__error_number = 0;

    SC_initialize_stmts(self, FALSE);
    cancelNeedDataState(self);
    self->cancel_info = 0;
    /*
     *      reset the current attr setting to the original one.
     */
    self->options.scroll_concurrency =
	self->options_orig.scroll_concurrency;
    self->options.cursor_type = self->options_orig.cursor_type;
    self->options.keyset_size = self->options_orig.keyset_size;
    self->options.maxLength = self->options_orig.maxLength;
    self->options.maxRows = self->options_orig.maxRows;

    return TRUE;
}


/*
 * Pre-execute a statement (for SQLPrepare/SQLDescribeCol) 
 */
Int4				/* returns # of fields if successful */
SC_pre_execute(StatementClass * self)
{
    Int4 num_fields = -1;
    QResultClass *res;
    mylog("SC_pre_execute: status = %d\n", self->status);

    res = SC_get_Curres(self);
    if (res && (num_fields = QR_NumResultCols(res)) > 0)
	return num_fields;

    mylog("SC_pre_execute: Unknown result/number of columns\n");
    return -1;
}


/* This is only called from SQLFreeStmt(SQL_UNBIND) */
char SC_unbind_cols(StatementClass * self)
{
    ARDFields *opts = SC_get_ARDF(self);
    GetDataInfo *gdata = SC_get_GDTI(self);
    BindInfoClass *bookmark;

    ARD_unbind_cols(opts, FALSE);
    GDATA_unbind_cols(gdata, FALSE);
    if (bookmark = opts->bookmark, bookmark != NULL)
    {
	bookmark->buffer = NULL;
	bookmark->used = NULL;
    }

    return 1;
}


void SC_clear_error(StatementClass * self)
{
    QResultClass *res;

    self->__error_number = 0;
    if (self->__error_message)
    {
	free(self->__error_message);
	self->__error_message = NULL;
    }
    if (self->pgerror)
    {
	ER_Destructor(self->pgerror);
	self->pgerror = NULL;
    }
    self->diag_row_count = 0;
    if (res = SC_get_Curres(self), res)
    {
	QR_set_message(res, NULL);
	QR_set_notice(res, NULL);
	res->sqlstate[0] = '\0';
    }
    self->stmt_time = 0;
    SC_unref_CC_error(self);
}


/*
 *	This function creates an error info which is the concatenation
 *	of the result, statement, connection, and socket messages.
 */

/*	Map sql commands to statement types */
static struct {
    int number;
    const char *ver3str;
    const char *ver2str;
} Statement_sqlstate[] =
{
    {
    STMT_ERROR_IN_ROW, "01S01", "01S01"},
    {
    STMT_OPTION_VALUE_CHANGED, "01S02", "01S02"},
    {
    STMT_ROW_VERSION_CHANGED, "01001", "01001"},	/* data changed */
    {
    STMT_POS_BEFORE_RECORDSET, "01S06", "01S06"},
    {
    STMT_TRUNCATED, "01004", "01004"},	/* data truncated */
    {
    STMT_INFO_ONLY, "00000", "00000"},	/* just an information that is returned, no error */
    {
    STMT_OK, "00000", "00000"},	/* OK */
    {
    STMT_EXEC_ERROR, "HY000", "S1000"},	/* also a general error */
    {
    STMT_STATUS_ERROR, "HY010", "S1010"},
    {
    STMT_SEQUENCE_ERROR, "HY010", "S1010"},	/* Function sequence error */
    {
    STMT_NO_MEMORY_ERROR, "HY001", "S1001"},	/* memory allocation failure */
    {
    STMT_COLNUM_ERROR, "07009", "S1002"},	/* invalid column number */
    {
    STMT_NO_STMTSTRING, "HY001", "S1001"},	/* having no stmtstring is also a malloc problem */
    {
    STMT_ERROR_TAKEN_FROM_BACKEND, "HY000", "S1000"},	/* general error */
    {
    STMT_INTERNAL_ERROR, "HY000", "S1000"},	/* general error */
    {
    STMT_STILL_EXECUTING, "HY010", "S1010"},
    {
    STMT_NOT_IMPLEMENTED_ERROR, "HYC00", "S1C00"},	/* == 'driver not 
							 * capable' */
    {
    STMT_BAD_PARAMETER_NUMBER_ERROR, "07009", "S1093"},
    {
    STMT_OPTION_OUT_OF_RANGE_ERROR, "HY092", "S1092"},
    {
    STMT_INVALID_COLUMN_NUMBER_ERROR, "07009", "S1002"},
    {
    STMT_RESTRICTED_DATA_TYPE_ERROR, "07006", "07006"},
    {
    STMT_INVALID_CURSOR_STATE_ERROR, "07005", "24000"},
    {
    STMT_CREATE_TABLE_ERROR, "42S01", "S0001"},	/* table already exists */
    {
    STMT_NO_CURSOR_NAME, "S1015", "S1015"},
    {
    STMT_INVALID_CURSOR_NAME, "34000", "34000"},
    {
    STMT_INVALID_ARGUMENT_NO, "HY024", "S1009"},	/* invalid argument value */
    {
    STMT_ROW_OUT_OF_RANGE, "HY107", "S1107"},
    {
    STMT_OPERATION_CANCELLED, "HY008", "S1008"},
    {
    STMT_INVALID_CURSOR_POSITION, "HY109", "S1109"},
    {
    STMT_VALUE_OUT_OF_RANGE, "HY019", "22003"},
    {
    STMT_OPERATION_INVALID, "HY011", "S1011"},
    {
    STMT_PROGRAM_TYPE_OUT_OF_RANGE, "?????", "?????"},
    {
    STMT_BAD_ERROR, "08S01", "08S01"},	/* communication link failure */
    {
    STMT_INVALID_OPTION_IDENTIFIER, "HY092", "HY092"},
    {
    STMT_RETURN_NULL_WITHOUT_INDICATOR, "22002", "22002"},
    {
    STMT_INVALID_DESCRIPTOR_IDENTIFIER, "HY091", "HY091"},
    {
    STMT_OPTION_NOT_FOR_THE_DRIVER, "HYC00", "HYC00"},
    {
    STMT_FETCH_OUT_OF_RANGE, "HY106", "S1106"},
    {
    STMT_COUNT_FIELD_INCORRECT, "07002", "07002"},
    {
    STMT_INVALID_NULL_ARG, "HY009", "S1009"}
};

static PG_ErrorInfo *SC_create_errorinfo(const StatementClass * self)
{
    QResultClass *res = SC_get_Curres(self);
    ConnectionClass *conn = SC_get_conn(self);
    Int4 errornum;
    size_t pos;
    BOOL resmsg = FALSE, detailmsg = FALSE, msgend = FALSE;
    char msg[4096], *wmsg;
    char *ermsg = NULL, *sqlstate = NULL;
    PG_ErrorInfo *pgerror;

    if (self->pgerror)
	return self->pgerror;
    errornum = self->__error_number;
    if (errornum == 0)
	return NULL;

    msg[0] = '\0';
    if (res)
    {
	if (res->sqlstate[0])
	    sqlstate = res->sqlstate;
	if (res->message)
	{
	    strncpy(msg, res->message, sizeof(msg));
	    detailmsg = resmsg = TRUE;
	}
	if (msg[0])
	    ermsg = msg;
	else if (QR_get_notice(res))
	{
	    char *notice = QR_get_notice(res);
	    size_t len = strlen(notice);
	    if (len < sizeof(msg))
	    {
		memcpy(msg, notice, len);
		msg[len] = '\0';
		ermsg = msg;
	    } else
	    {
		ermsg = notice;
		msgend = TRUE;
	    }
	}
    }
    if (!msgend && (wmsg = SC_get_errormsg(self)) && wmsg[0])
    {
	pos = strlen(msg);

	if (detailmsg)
	{
	    msg[pos++] = ';';
	    msg[pos++] = '\n';
	}
	strncpy(msg + pos, wmsg, sizeof(msg) - pos);
	ermsg = msg;
	detailmsg = TRUE;
    }
    if (!self->ref_CC_error)
	msgend = TRUE;

    if (conn && !msgend)
    {
	SocketClass *sock = conn->sock;
	const char *sockerrmsg;

	if (!resmsg && (wmsg = CC_get_errormsg(conn))
	    && wmsg[0] != '\0')
	{
	    pos = strlen(msg);
	    snprintf(&msg[pos], sizeof(msg) - pos, ";\n%s",
		     CC_get_errormsg(conn));
	}

	if (sock && NULL != (sockerrmsg = SOCK_get_errmsg(sock))
	    && '\0' != sockerrmsg[0])
	{
	    pos = strlen(msg);
	    snprintf(&msg[pos], sizeof(msg) - pos, ";\n%s", sockerrmsg);
	}
	ermsg = msg;
    }
    pgerror = ER_Constructor(self->__error_number, ermsg);
    if (sqlstate)
	strcpy(pgerror->sqlstate, sqlstate);
    else if (conn)
    {
	if (!msgend && conn->sqlstate[0])
	    strcpy(pgerror->sqlstate, conn->sqlstate);
	else
	{
	    EnvironmentClass *env = (EnvironmentClass *) conn->henv;

	    errornum -= LOWEST_STMT_ERROR;
	    if (errornum < 0 ||
		errornum >=
		sizeof(Statement_sqlstate) /
		sizeof(Statement_sqlstate[0]))
		errornum = 1 - LOWEST_STMT_ERROR;
	    strcpy(pgerror->sqlstate, EN_is_odbc3(env) ?
		   Statement_sqlstate[errornum].ver3str :
		   Statement_sqlstate[errornum].ver2str);
	}
    }

    return pgerror;
}


StatementClass *SC_get_ancestor(StatementClass * stmt)
{
    StatementClass *child = stmt, *parent;

    inolog("SC_get_ancestor in stmt=%p\n", stmt);
    for (child = stmt, parent = child->execute_parent; parent;
	 child = parent, parent = child->execute_parent)
    {
	inolog("parent=%p\n", parent);
    }
    return child;
}

void SC_reset_delegate(RETCODE retcode, StatementClass * stmt)
{
    StatementClass *delegate = stmt->execute_delegate;

    if (!delegate)
	return;
    PGAPI_FreeStmt(delegate, SQL_DROP);
}

void
SC_set_error(StatementClass * self, int number, const char *message,
	     const char *func)
{
    if (self->__error_message)
	free(self->__error_message);
    self->__error_number = number;
    self->__error_message = message ? strdup(message) : NULL;
    if (func && number != STMT_OK && number != STMT_INFO_ONLY)
	SC_log_error(func, "", self);
}


void SC_set_errormsg(StatementClass * self, const char *message)
{
    if (self->__error_message)
	free(self->__error_message);
    self->__error_message = message ? strdup(message) : NULL;
}


void
SC_replace_error_with_res(StatementClass * self, int number,
			  const char *message,
			  const QResultClass * from_res, BOOL check)
{
    QResultClass *self_res;
    BOOL repstate;

    inolog("SC_set_error_from_res %p->%p check=%i\n", from_res, self,
	   check);
    if (check)
    {
	if (0 == number)
	    return;
	if (0 > number &&	/* SQL_SUCCESS_WITH_INFO */
	    0 < self->__error_number)
	    return;
    }
    self->__error_number = number;
    if (!check || message)
    {
	if (self->__error_message)
	    free(self->__error_message);
	self->__error_message = message ? strdup(message) : NULL;
    }
    if (self->pgerror)
    {
	ER_Destructor(self->pgerror);
	self->pgerror = NULL;
    }
    self_res = SC_get_Curres(self);
    if (!self_res)
	return;
    if (self_res == from_res)
	return;
    QR_add_message(self_res, QR_get_message(from_res));
    QR_add_notice(self_res, QR_get_notice(from_res));
    repstate = FALSE;
    if (!check)
	repstate = TRUE;
    else if (from_res->sqlstate[0])
    {
	if (!self_res->sqlstate[0]
	    || strncmp(self_res->sqlstate, "00", 2) == 0)
	    repstate = TRUE;
	else if (strncmp(from_res->sqlstate, "01", 2) >= 0)
	    repstate = TRUE;
    }
    if (repstate)
	strcpy(self_res->sqlstate, from_res->sqlstate);
}

void
SC_error_copy(StatementClass * self, const StatementClass * from,
	      BOOL check)
{
    QResultClass *self_res, *from_res;
    BOOL repstate;

    inolog("SC_error_copy %p->%p check=%i\n", from, self, check);
    if (self == from)
	return;
    if (check)
    {
	if (0 == from->__error_number)	/* SQL_SUCCESS */
	    return;
	if (0 > from->__error_number &&	/* SQL_SUCCESS_WITH_INFO */
	    0 < self->__error_number)
	    return;
    }
    self->__error_number = from->__error_number;
    if (!check || from->__error_message)
    {
	if (self->__error_message)
	    free(self->__error_message);
	self->__error_message =
	    from->__error_message ? strdup(from->
					   __error_message) : NULL;
    }
    if (self->pgerror)
    {
	ER_Destructor(self->pgerror);
	self->pgerror = NULL;
    }
    self_res = SC_get_Curres(self);
    from_res = SC_get_Curres(from);
    if (!self_res || !from_res)
	return;
    QR_add_message(self_res, QR_get_message(from_res));
    QR_add_notice(self_res, QR_get_notice(from_res));
    repstate = FALSE;
    if (!check)
	repstate = TRUE;
    else if (from_res->sqlstate[0])
    {
	if (!self_res->sqlstate[0]
	    || strncmp(self_res->sqlstate, "00", 2) == 0)
	    repstate = TRUE;
	else if (strncmp(from_res->sqlstate, "01", 2) >= 0)
	    repstate = TRUE;
    }
    if (repstate)
	strcpy(self_res->sqlstate, from_res->sqlstate);
}


void
SC_full_error_copy(StatementClass * self, const StatementClass * from,
		   BOOL allres)
{
    PG_ErrorInfo *pgerror;

    inolog("SC_full_error_copy %p->%p\n", from, self);
    if (self->__error_message)
    {
	free(self->__error_message);
	self->__error_message = NULL;
    }
    if (from->__error_message)
	self->__error_message = strdup(from->__error_message);
    self->__error_number = from->__error_number;
    if (from->pgerror)
    {
	if (self->pgerror)
	    ER_Destructor(self->pgerror);
	self->pgerror = ER_Dup(from->pgerror);
	return;
    } else if (!allres)
	return;
    pgerror = SC_create_errorinfo(from);
    if (!pgerror->__error_message[0])
    {
	ER_Destructor(pgerror);
	return;
    }
    if (self->pgerror)
	ER_Destructor(self->pgerror);
    self->pgerror = pgerror;
}

/*              Returns the next SQL error information. */
RETCODE SQL_API
PGAPI_StmtError(SQLHSTMT hstmt,
		SQLSMALLINT RecNumber,
		SQLCHAR FAR * szSqlState,
		SQLINTEGER FAR * pfNativeError,
		SQLCHAR FAR * szErrorMsg,
		SQLSMALLINT cbErrorMsgMax,
		SQLSMALLINT FAR * pcbErrorMsg, UWORD flag)
{
    /* CC: return an error of a hdesc  */
    StatementClass *stmt = (StatementClass *) hstmt;

    stmt->pgerror = SC_create_errorinfo(stmt);
    return ER_ReturnError(&(stmt->pgerror), RecNumber, szSqlState,
			  pfNativeError, szErrorMsg, cbErrorMsgMax,
			  pcbErrorMsg, flag);
}

time_t SC_get_time(StatementClass * stmt)
{
    if (!stmt)
	return time(NULL);
    if (0 == stmt->stmt_time)
	stmt->stmt_time = time(NULL);
    return stmt->stmt_time;
}

/*
 *	Currently, the driver offers very simple bookmark support -- it is
 *	just the current row number.  But it could be more sophisticated
 *	someday, such as mapping a key to a 32 bit value
 */
SQLULEN SC_get_bookmark(StatementClass * self)
{
    return SC_make_bookmark(self->currTuple);
}


RETCODE SC_fetch(StatementClass * self)
{
    CSTR func = "SC_fetch";
    QResultClass *res = SC_get_Curres(self);
    ARDFields *opts;
    GetDataInfo *gdata;
    int retval;
    RETCODE result;

    Int2 num_cols, lf;
    OID type;
    char *value;
    ColumnInfoClass *coli;
    BindInfoClass *bookmark;

    inolog("%s statement=%p ommitted=0\n", func, self);
    self->last_fetch_count = self->last_fetch_count_include_ommitted =
	0;
    coli = QR_get_fields(res);	/* the column info */

    mylog("fetch_cursor=%d, %p->total_read=%d\n",
	  0 /*SC_is_fetchcursor(self)*/, res, res->num_total_read);

    if (self->currTuple >= (Int4) QR_get_num_total_tuples(res) - 1
	|| (self->options.maxRows > 0
	    && self->currTuple == self->options.maxRows - 1))
    {
	/*
	 * if at the end of the tuples, return "no data found" and set
	 * the cursor past the end of the result set
	 */
	self->currTuple = QR_get_num_total_tuples(res);
	return SQL_NO_DATA_FOUND;
    }

    mylog("**** %s: non-cursor_result\n", func);
    (self->currTuple)++;

    if (QR_haskeyset(res))
    {
	SQLLEN kres_ridx;

	kres_ridx = GIdx2KResIdx(self->currTuple, self, res);
	if (kres_ridx >= 0 && kres_ridx < res->num_cached_keys)
	{
	    UWORD pstatus = res->keyset[kres_ridx].status;
	    inolog("SC_ pstatus[%d]=%hx fetch_count=" FORMAT_LEN "\n",
		   kres_ridx, pstatus, self->last_fetch_count);
	    if (0 !=
		(pstatus & (CURS_SELF_DELETING | CURS_SELF_DELETED)))
		return SQL_SUCCESS_WITH_INFO;
	    if (SQL_ROW_DELETED != (pstatus & KEYSET_INFO_PUBLIC) &&
		0 != (pstatus & CURS_OTHER_DELETED))
		return SQL_SUCCESS_WITH_INFO;
	    if (0 != (CURS_NEEDS_REREAD & pstatus))
	    {
		UWORD qcount;

		result =
		    SC_pos_reload(self, self->currTuple, &qcount, 0);
		if (SQL_ERROR == result)
		    return result;
		pstatus &= ~CURS_NEEDS_REREAD;
	    }
	}
    }

    num_cols = QR_NumPublicResultCols(res);

    result = SQL_SUCCESS;
    self->last_fetch_count++;
    inolog("%s: stmt=%p ommitted++\n", func, self);
    self->last_fetch_count_include_ommitted++;

    opts = SC_get_ARDF(self);
    /*
     * If the bookmark column was bound then return a bookmark. Since this
     * is used with SQLExtendedFetch, and the rowset size may be greater
     * than 1, and an application can use row or column wise binding, use
     * the code in copy_and_convert_field() to handle that.
     */
    if ((bookmark = opts->bookmark) && bookmark->buffer)
    {
	char buf[32];
	SQLLEN offset =
	    opts->row_offset_ptr ? *opts->row_offset_ptr : 0;

	sprintf(buf, FORMAT_ULEN, SC_get_bookmark(self));
	SC_set_current_col(self, -1);
	result = copy_and_convert_field(self, 0, buf,
					SQL_C_ULONG,
					bookmark->buffer + offset, 0,
					LENADDR_SHIFT(bookmark->used,
						      offset),
					LENADDR_SHIFT(bookmark->used,
						      offset));
    }

    if (self->options.retrieve_data == SQL_RD_OFF)	/* data isn't required */
	return SQL_SUCCESS;
    gdata = SC_get_GDTI(self);
    if (gdata->allocated != opts->allocated)
	extend_getdata_info(gdata, opts->allocated, TRUE);
    for (lf = 0; lf < num_cols; lf++)
    {
	mylog
	    ("fetch: cols=%d, lf=%d, opts = %p, opts->bindings = %p, buffer[] = %p\n",
	     num_cols, lf, opts, opts->bindings,
	     opts->bindings[lf].buffer);

	/* reset for SQLGetData */
	gdata->gdata[lf].data_left = -1;

	if (NULL == opts->bindings)
	    continue;
	if (opts->bindings[lf].buffer != NULL)
	{
	    /* this column has a binding */

	    /* type = QR_get_field_type(res, lf); */
	    type = CI_get_oid(coli, lf);	/* speed things up */

	    mylog("type = %d\n", type);

	    SQLLEN curt = GIdx2CacheIdx(self->currTuple, self, res);
	    inolog("base=%d curr=%d st=%d\n",
		   QR_get_rowstart_in_cache(res), self->currTuple,
		   SC_get_rowset_start(self));
	    inolog("curt=%d\n", curt);
	    value = (char *)QR_get_value_backend_row(res, curt, lf);

	    mylog("value = '%s'\n", (value == NULL) ? "<NULL>" : value);

	    retval =
		copy_and_convert_field_bindinfo(self, type, value, lf);

	    mylog("copy_and_convert: retval = %d\n", retval);

	    switch (retval)
	    {
	    case COPY_OK:
		break;		/* OK, do next bound column */

	    case COPY_UNSUPPORTED_TYPE:
		SC_set_error(self, STMT_RESTRICTED_DATA_TYPE_ERROR,
			     "Received an unsupported type from Postgres.",
			     func);
		result = SQL_ERROR;
		break;

	    case COPY_UNSUPPORTED_CONVERSION:
		SC_set_error(self, STMT_RESTRICTED_DATA_TYPE_ERROR,
			     "Couldn't handle the necessary data type conversion.",
			     func);
		result = SQL_ERROR;
		break;

	    case COPY_RESULT_TRUNCATED:
		SC_set_error(self, STMT_TRUNCATED,
			     "Fetched item was truncated.", func);
		qlog("The %dth item was truncated\n", lf + 1);
		qlog("The buffer size = %d", opts->bindings[lf].buflen);
		qlog(" and the value is '%s'\n", value);
		result = SQL_SUCCESS_WITH_INFO;
		break;

		/* error msg already filled in */
	    case COPY_GENERAL_ERROR:
		result = SQL_ERROR;
		break;

		/* This would not be meaningful in SQLFetch. */
	    case COPY_NO_DATA_FOUND:
		break;

	    default:
		SC_set_error(self, STMT_INTERNAL_ERROR,
			     "Unrecognized return value from copy_and_convert_field.",
			     func);
		result = SQL_ERROR;
		break;
	    }
	}
    }

    return result;
}


#define	CALLBACK_ALLOC_ONCE	4
int enqueueNeedDataCallback(StatementClass * stmt,
			    NeedDataCallfunc func, void *data)
{
    if (stmt->num_callbacks >= stmt->allocated_callbacks)
    {
	stmt->callbacks = (NeedDataCallback *) realloc(stmt->callbacks,
						       sizeof
						       (NeedDataCallback)
						       *
						       (stmt->
							allocated_callbacks
							+
							CALLBACK_ALLOC_ONCE));
	stmt->allocated_callbacks += CALLBACK_ALLOC_ONCE;
    }
    stmt->callbacks[stmt->num_callbacks].func = func;
    stmt->callbacks[stmt->num_callbacks].data = data;
    stmt->num_callbacks++;

    inolog("enqueueNeedDataCallack stmt=%p, func=%p, count=%d\n", stmt,
	   func, stmt->num_callbacks);
    return stmt->num_callbacks;
}

RETCODE dequeueNeedDataCallback(RETCODE retcode, StatementClass * stmt)
{
    RETCODE ret;
    NeedDataCallfunc func;
    void *data;
    int i, cnt;

    mylog("dequeueNeedDataCallback ret=%d count=%d\n", retcode,
	  stmt->num_callbacks);
    if (SQL_NEED_DATA == retcode)
	return retcode;
    if (stmt->num_callbacks <= 0)
	return retcode;
    func = stmt->callbacks[0].func;
    data = stmt->callbacks[0].data;
    for (i = 1; i < stmt->num_callbacks; i++)
	stmt->callbacks[i - 1] = stmt->callbacks[i];
    cnt = --stmt->num_callbacks;
    ret = (*func) (retcode, data);
    free(data);
    if (SQL_NEED_DATA != ret && cnt > 0)
	ret = dequeueNeedDataCallback(ret, stmt);
    return ret;
}

void cancelNeedDataState(StatementClass * stmt)
{
    int cnt = stmt->num_callbacks, i;

    stmt->num_callbacks = 0;
    for (i = 0; i < cnt; i++)
    {
	if (stmt->callbacks[i].data)
	    free(stmt->callbacks[i].data);
    }
    SC_reset_delegate(SQL_ERROR, stmt);
}

void
SC_log_error(const char *func, const char *desc,
	     const StatementClass * self)
{
    const char *head;
#ifdef PRN_NULLCHECK
#define nullcheck(a) (a ? a : "(NULL)")
#endif
    if (self)
    {
	QResultClass *res = SC_get_Result(self);
	const ARDFields *opts = SC_get_ARDF(self);
	const APDFields *apdopts = SC_get_APDF(self);
	SQLLEN rowsetSize;

	rowsetSize =
	    (7 ==
	     self->transition_status ? opts->
	     size_of_rowset_odbc2 : opts->size_of_rowset);
	if (SC_get_errornumber(self) <= 0)
	    head = "STATEMENT WARNING";
	else
	{
	    head = "STATEMENT ERROR";
	    qlog("%s: func=%s, desc='%s', errnum=%d, errmsg='%s'\n",
		 head, func, desc, self->__error_number,
		 nullcheck(self->__error_message));
	}
	mylog("%s: func=%s, desc='%s', errnum=%d, errmsg='%s'\n", head,
	      func, desc, self->__error_number,
	      nullcheck(self->__error_message));
	if (SC_get_errornumber(self) > 0)
	{
	    qlog("                 ------------------------------------------------------------\n");
	    qlog("                 hdbc=%p, stmt=%p, result=%p\n",
		 self->hdbc, self, res);
	    qlog("                 prepare=%d, internal=%d\n",
		 self->prepare, self->internal);
	    qlog("                 bindings=%p, bindings_allocated=%d\n", opts->bindings, opts->allocated);
	    qlog("                 parameters=%p, parameters_allocated=%d\n", apdopts->parameters, apdopts->allocated);
	    qlog("                 statement_type=%d, statement='%s'\n",
		 self->statement_type, nullcheck(self->statement));
	    qlog("                 stmt_with_params='%s'\n",
		 nullcheck(self->stmt_with_params));
	    qlog("                 data_at_exec=%d, current_exec_param=%d, put_data=%d\n", self->data_at_exec, self->current_exec_param, self->put_data);
	    qlog("                 currTuple=%d, current_col=%d\n", self->currTuple, self->current_col);
	    qlog("                 maxRows=%d, rowset_size=%d, keyset_size=%d, cursor_type=%d, scroll_concurrency=%d\n", self->options.maxRows, rowsetSize, self->options.keyset_size, self->options.cursor_type, self->options.scroll_concurrency);
	    qlog("                 cursor_name='%s'\n",
		 SC_cursor_name(self));

	    qlog("                 ----------------QResult Info -------------------------------\n");

	    if (res)
	    {
		qlog("                 fields=%p, backend_tuples=%p, tupleField=%d, conn=%p\n", res->fields, res->backend_tuples, res->tupleField, res->conn);
		qlog("                 fetch_count=%d, num_total_rows=%d, num_fields=%d, cursor='%s'\n", res->fetch_number, QR_get_num_total_tuples(res), res->num_fields, nullcheck(QR_get_cursor(res)));
		qlog("                 message='%s', command='%s', notice='%s'\n", nullcheck(res->message), nullcheck(res->command), nullcheck(res->notice));
		qlog("                 status=%d, inTuples=%d\n",
		     QR_get_rstatus(res), QR_is_fetching_tuples(res));
	    }

	    /* Log the connection error if there is one */
	    CC_log_error(func, desc, self->hdbc);
	}
    } else
    {
	qlog("INVALID STATEMENT HANDLE ERROR: func=%s, desc='%s'\n",
	     func, desc);
	mylog("INVALID STATEMENT HANDLE ERROR: func=%s, desc='%s'\n",
	      func, desc);
    }
#undef PRN_NULLCHECK
}

/*
 *	Extended Query 
 */

enum {
    CancelRequestSet = 1L, CancelRequestAccepted =
	(1L << 1), CancelCompleted = (1L << 2)
};
/*	commonly used for short term lock */
#if defined(WIN_MULTITHREAD_SUPPORT)
extern CRITICAL_SECTION common_cs;
#elif defined(POSIX_MULTITHREAD_SUPPORT)
extern pthread_mutex_t common_cs;
#endif				/* WIN_MULTITHREAD_SUPPORT */
BOOL SC_IsExecuting(const StatementClass * self)
{
    BOOL ret;
    ENTER_COMMON_CS;		/* short time blocking */
    ret = (STMT_EXECUTING == self->status);
    LEAVE_COMMON_CS;
    return ret;
}

BOOL SC_SetExecuting(StatementClass * self, BOOL on)
{
    BOOL exeSet = FALSE;
    ENTER_COMMON_CS;		/* short time blocking */
    if (on)
    {
	if (0 == (self->cancel_info & CancelRequestSet))
	{
	    self->status = STMT_EXECUTING;
	    exeSet = TRUE;
	}
    } else
    {
	self->cancel_info = 0;
	self->status = STMT_FINISHED;
	exeSet = TRUE;
    }
    LEAVE_COMMON_CS;
    return exeSet;
}

BOOL SC_SetCancelRequest(StatementClass * self)
{
    BOOL enteredCS = FALSE;

    ENTER_COMMON_CS;
    if (0 != (self->cancel_info & CancelCompleted))
	;
    else if (STMT_EXECUTING == self->status)
    {
	self->cancel_info |= CancelRequestSet;
    } else
    {
	/* try to acquire */
	if (TRY_ENTER_STMT_CS(self))
	    enteredCS = TRUE;
	else
	    self->cancel_info |= CancelRequestSet;
    }
    LEAVE_COMMON_CS;
    return enteredCS;
}

BOOL SC_AcceptedCancelRequest(const StatementClass * self)
{
    BOOL shouldCancel = FALSE;
    ENTER_COMMON_CS;
    if (0 !=
	(self->
	 cancel_info & (CancelRequestSet | CancelRequestAccepted |
			CancelCompleted)))
	shouldCancel = TRUE;
    LEAVE_COMMON_CS;
    return shouldCancel;
}
