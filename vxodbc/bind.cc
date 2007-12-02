/*
 * Description:	This module contains routines related to binding
 *		columns and parameters.
 */
#include <string.h>
#include <ctype.h>
#include "bind.h"
#include "misc.h"

#include "environ.h"
#include "statement.h"
#include "descriptor.h"
#include "qresult.h"
#include "pgtypes.h"
#include "multibyte.h"

#include "pgapifunc.h"


/*		Bind parameters on a statement handle */
RETCODE SQL_API
PGAPI_BindParameter(HSTMT hstmt,
		    SQLUSMALLINT ipar,
		    SQLSMALLINT fParamType,
		    SQLSMALLINT fCType,
		    SQLSMALLINT fSqlType,
		    SQLULEN cbColDef,
		    SQLSMALLINT ibScale,
		    PTR rgbValue,
		    SQLLEN cbValueMax, SQLLEN FAR * pcbValue)
{
    StatementClass *stmt = (StatementClass *) hstmt;
    CSTR func = "PGAPI_BindParameter";
    mylog("PGAPI_BindParameter not yet implemented");
    SC_log_error(func, "PGAPI_BindParameter not yet implemented", stmt);
    return SQL_ERROR;
}


/*	Associate a user-supplied buffer with a database column. */
RETCODE SQL_API
PGAPI_BindCol(HSTMT hstmt,
	      SQLUSMALLINT icol,
	      SQLSMALLINT fCType,
	      PTR rgbValue, SQLLEN cbValueMax, SQLLEN FAR * pcbValue)
{
    CSTR func = "PGAPI_BindCol";
    StatementClass *stmt = (StatementClass *) hstmt;
    ARDFields *opts;
    GetDataInfo *gdata_info;
    BindInfoClass *bookmark;
    RETCODE ret = SQL_SUCCESS;

    mylog("Entering(%p, %d)\n", stmt, icol);
    mylog("fCType=%d rgb=%p valusMax=%d pcb=%p\n",
	  fCType, rgbValue, cbValueMax, pcbValue);

    if (!stmt)
    {
	SC_log_error(func, "", NULL);
	return SQL_INVALID_HANDLE;
    }

    opts = SC_get_ARDF(stmt);
    if (stmt->status == STMT_EXECUTING)
    {
	SC_set_error(stmt, STMT_SEQUENCE_ERROR,
		     "Can't bind columns while statement is still executing.",
		     func);
	return SQL_ERROR;
    }
#define	return	DONT_CALL_RETURN_FROM_HERE ???
    SC_clear_error(stmt);
    /* If the bookmark column is being bound, then just save it */
    if (icol == 0)
    {
	bookmark = opts->bookmark;
	if (rgbValue == NULL)
	{
	    if (bookmark)
	    {
		bookmark->buffer = NULL;
		bookmark->used = bookmark->indicator = NULL;
	    }
	}
	else
	{
	    /* Make sure it is the bookmark data type */
	    switch (fCType)
	    {
	    case SQL_C_BOOKMARK:
	    case SQL_C_VARBOOKMARK:
		break;
	    default:
		SC_set_error(stmt, STMT_PROGRAM_TYPE_OUT_OF_RANGE,
			     "Bind column 0 is not of type SQL_C_BOOKMARK",
			     func);
		inolog
		    ("Bind column 0 is type %d not of type SQL_C_BOOKMARK",
		     fCType);
		ret = SQL_ERROR;
		goto cleanup;
	    }

	    bookmark = ARD_AllocBookmark(opts);
	    bookmark->buffer = (char *)rgbValue;
	    bookmark->used = bookmark->indicator = pcbValue;
	    bookmark->buflen = cbValueMax;
	    bookmark->returntype = fCType;
	}
	goto cleanup;
    }

    /*
     * Allocate enough bindings if not already done. Most likely,
     * execution of a statement would have setup the necessary bindings.
     * But some apps call BindCol before any statement is executed.
     */
    if (icol > opts->allocated)
	extend_column_bindings(opts, icol);
    gdata_info = SC_get_GDTI(stmt);
    if (icol > gdata_info->allocated)
	extend_getdata_info(gdata_info, icol, FALSE);

    /* check to see if the bindings were allocated */
    if (!opts->bindings)
    {
	SC_set_error(stmt, STMT_NO_MEMORY_ERROR,
		     "Could not allocate memory for bindings.", func);
	ret = SQL_ERROR;
	goto cleanup;
    }

    /* use zero based col numbers from here out */
    icol--;

    /* Reset for SQLGetData */
    gdata_info->gdata[icol].data_left = -1;

    if (rgbValue == NULL)
    {
	/* we have to unbind the column */
	opts->bindings[icol].buflen = 0;
	opts->bindings[icol].buffer = NULL;
	opts->bindings[icol].used =
	    opts->bindings[icol].indicator = NULL;
	opts->bindings[icol].returntype = SQL_C_CHAR;
	opts->bindings[icol].precision = 0;
	opts->bindings[icol].scale = 0;
	if (gdata_info->gdata[icol].ttlbuf)
	    free(gdata_info->gdata[icol].ttlbuf);
	gdata_info->gdata[icol].ttlbuf = NULL;
	gdata_info->gdata[icol].ttlbuflen = 0;
	gdata_info->gdata[icol].ttlbufused = 0;
    }
    else
    {
	/* ok, bind that column */
	opts->bindings[icol].buflen = cbValueMax;
	opts->bindings[icol].buffer = (char *)rgbValue;
	opts->bindings[icol].used =
	    opts->bindings[icol].indicator = pcbValue;
	opts->bindings[icol].returntype = fCType;
	if (SQL_C_NUMERIC == fCType)
	    opts->bindings[icol].precision = 32;
	else
	    opts->bindings[icol].precision = 0;
	opts->bindings[icol].scale = 0;

	mylog("bound buffer[%d] = %p\n", icol,
	      opts->bindings[icol].buffer);
    }

  cleanup:
#undef	return
    if (stmt->internal)
	ret = DiscardStatementSvp(stmt, ret, FALSE);
    return ret;
}


/*
 *	Returns the description of a parameter marker.
 *	This function is listed as not being supported by SQLGetFunctions() because it is
 *	used to describe "parameter markers" (not bound parameters), in which case,
 *	the dbms should return info on the markers.  Since Postgres doesn't support that,
 *	it is best to say this function is not supported and let the application assume a
 *	data type (most likely varchar).
 */
RETCODE SQL_API
PGAPI_DescribeParam(HSTMT hstmt,
		    SQLUSMALLINT ipar,
		    SQLSMALLINT FAR * pfSqlType,
		    SQLULEN FAR * pcbParamDef,
		    SQLSMALLINT FAR * pibScale,
		    SQLSMALLINT FAR * pfNullable)
{
    StatementClass *stmt = (StatementClass *) hstmt;
    CSTR func = "PGAPI_DescribeParam";
    SC_set_error(stmt, STMT_EXEC_ERROR,
		 "SQLDescribeParam is not yet supported", func);
    return SQL_ERROR;
}


/*
 * This function should really talk to the dbms to determine the number of
 * "parameter markers" (not bound parameters) in the statement.  But, since
 * Postgres doesn't support that, the driver should just count the number
 * of markers and return that.  The reason the driver just can't say this
 * function is unsupported like it does for SQLDescribeParam is that some
 * applications don't care and try to call it anyway. If the statement does
 * not have parameters, it should just return 0.
 */
RETCODE SQL_API PGAPI_NumParams(HSTMT hstmt, SQLSMALLINT FAR * pcpar)
{
    StatementClass *stmt = (StatementClass *) hstmt;
    CSTR func = "PGAPI_NumParams";
    
    SC_set_error(stmt, STMT_EXEC_ERROR,
		 "PGAPI_NumParams is not yet supported", func);
    return SQL_ERROR;
}


/*
 *	 Bindings Implementation
 */
static BindInfoClass *create_empty_bindings(int num_columns)
{
    BindInfoClass *new_bindings;
    int i;

    new_bindings =
	(BindInfoClass *) malloc(num_columns * sizeof(BindInfoClass));
    if (!new_bindings)
	return NULL;

    for (i = 0; i < num_columns; i++)
    {
	new_bindings[i].buflen = 0;
	new_bindings[i].buffer = NULL;
	new_bindings[i].used = new_bindings[i].indicator = NULL;
    }

    return new_bindings;
}


int CountParameters(const StatementClass * self, Int2 * inputCount,
		    Int2 * ioCount, Int2 * outputCount)
{
    IPDFields *ipdopts = SC_get_IPDF(self);
    int i, num_params, valid_count;

    if (inputCount)
	*inputCount = 0;
    if (ioCount)
	*ioCount = 0;
    if (outputCount)
	*outputCount = 0;
    if (!ipdopts)
	return -1;
    num_params = self->num_params;
    if (ipdopts->allocated < num_params)
	num_params = ipdopts->allocated;
    for (i = 0, valid_count = 0; i < num_params; i++)
    {
	if (SQL_PARAM_OUTPUT == ipdopts->parameters[i].paramType)
	{
	    if (outputCount)
	    {
		(*outputCount)++;
		valid_count++;
	    }
	} else if (SQL_PARAM_INPUT_OUTPUT ==
		   ipdopts->parameters[i].paramType)
	{
	    if (ioCount)
	    {
		(*ioCount)++;
		valid_count++;
	    }
	} else if (inputCount)
	{
	    (*inputCount)++;
	    valid_count++;
	}
    }
    return valid_count;
}

/*
 *	Free parameters and free the memory.
 */
void APD_free_params(APDFields * apdopts, char option)
{
    CSTR func = "APD_free_params";
    mylog("%s:  ENTER, self=%p\n", func, apdopts);

    if (!apdopts->parameters)
	return;

    if (option == STMT_FREE_PARAMS_ALL)
    {
	free(apdopts->parameters);
	apdopts->parameters = NULL;
	apdopts->allocated = 0;
    }

    mylog("%s:  EXIT\n", func);
}

/*
 *	Free parameters and free the memory.
 */
void IPD_free_params(IPDFields * ipdopts, char option)
{
    CSTR func = "IPD_free_params";

    mylog("%s:  ENTER, self=%p\n", func, ipdopts);

    if (!ipdopts->parameters)
	return;
    if (option == STMT_FREE_PARAMS_ALL)
    {
	free(ipdopts->parameters);
	ipdopts->parameters = NULL;
	ipdopts->allocated = 0;
    }

    mylog("%s:  EXIT\n", func);
}

void extend_column_bindings(ARDFields * self, int num_columns)
{
    CSTR func = "extend_column_bindings";
    BindInfoClass *new_bindings;
    int i;

    mylog
	("%s: entering ... self=%p, bindings_allocated=%d, num_columns=%d\n",
	 func, self, self->allocated, num_columns);

    /*
     * if we have too few, allocate room for more, and copy the old
     * entries into the new structure
     */
    if (self->allocated < num_columns)
    {
	new_bindings = create_empty_bindings(num_columns);
	if (!new_bindings)
	{
	    mylog
		("%s: unable to create %d new bindings from %d old bindings\n",
		 func, num_columns, self->allocated);

	    if (self->bindings)
	    {
		free(self->bindings);
		self->bindings = NULL;
	    }
	    self->allocated = 0;
	    return;
	}

	if (self->bindings)
	{
	    for (i = 0; i < self->allocated; i++)
		new_bindings[i] = self->bindings[i];

	    free(self->bindings);
	}

	self->bindings = new_bindings;
	self->allocated = num_columns;
    }

    /*
     * There is no reason to zero out extra bindings if there are more
     * than needed.  If an app has allocated extra bindings, let it worry
     * about it by unbinding those columns.
     */

    /* SQLBindCol(1..) ... SQLBindCol(10...)   # got 10 bindings */
    /* SQLExecDirect(...)  # returns 5 cols */
    /* SQLExecDirect(...)  # returns 10 cols  (now OK) */

    mylog("exit %s=%p\n", func, self->bindings);
}

void reset_a_column_binding(ARDFields * self, int icol)
{
    BindInfoClass *bookmark;

    mylog("self=%p, allocated=%d, icol=%d\n",
	  self, self->allocated, icol);

    if (icol > self->allocated)
	return;

    /* use zero based col numbers from here out */
    if (0 == icol)
    {
	if (bookmark = self->bookmark, bookmark != NULL)
	{
	    bookmark->buffer = NULL;
	    bookmark->used = bookmark->indicator = NULL;
	}
    } else
    {
	icol--;

	/* we have to unbind the column */
	self->bindings[icol].buflen = 0;
	self->bindings[icol].buffer = NULL;
	self->bindings[icol].used =
	    self->bindings[icol].indicator = NULL;
	self->bindings[icol].returntype = SQL_C_CHAR;
    }
}

void ARD_unbind_cols(ARDFields * self, BOOL freeall)
{
    Int2 lf;

    inolog("ARD_unbind_cols freeall=%d allocated=%d bindings=%p",
	   freeall, self->allocated, self->bindings);
    for (lf = 1; lf <= self->allocated; lf++)
	reset_a_column_binding(self, lf);
    if (freeall)
    {
	if (self->bindings)
	    free(self->bindings);
	self->bindings = NULL;
	self->allocated = 0;
    }
}
void GDATA_unbind_cols(GetDataInfo * self, BOOL freeall)
{
    Int2 lf;

    inolog("GDATA_unbind_cols freeall=%d allocated=%d gdata=%p",
	   freeall, self->allocated, self->gdata);
    if (self->fdata.ttlbuf)
    {
	free(self->fdata.ttlbuf);
	self->fdata.ttlbuf = NULL;
    }
    self->fdata.ttlbuflen = self->fdata.ttlbufused = 0;
    self->fdata.data_left = -1;
    for (lf = 1; lf <= self->allocated; lf++)
	reset_a_getdata_info(self, lf);
    if (freeall)
    {
	if (self->gdata)
	    free(self->gdata);
	self->gdata = NULL;
	self->allocated = 0;
    }
}

void GetDataInfoInitialize(GetDataInfo * gdata_info)
{
    gdata_info->fdata.data_left = -1;
    gdata_info->fdata.ttlbuf = NULL;
    gdata_info->fdata.ttlbuflen = gdata_info->fdata.ttlbufused = 0;
    gdata_info->allocated = 0;
    gdata_info->gdata = NULL;
}
static GetDataClass *create_empty_gdata(int num_columns)
{
    GetDataClass *new_gdata;
    int i;

    new_gdata =
	(GetDataClass *) malloc(num_columns * sizeof(GetDataClass));
    if (!new_gdata)
	return NULL;

    for (i = 0; i < num_columns; i++)
    {
	new_gdata[i].data_left = -1;
	new_gdata[i].ttlbuf = NULL;
	new_gdata[i].ttlbuflen = 0;
	new_gdata[i].ttlbufused = 0;
    }

    return new_gdata;

}
void extend_getdata_info(GetDataInfo * self, int num_columns, BOOL shrink)
{
    GetDataClass *new_gdata;

    mylog("self=%p, allocated=%d, num_columns=%d\n",
	  self, self->allocated, num_columns);

    /*
     * if we have too few, allocate room for more, and copy the old
     * entries into the new structure
     */
    if (self->allocated < num_columns)
    {
	new_gdata = create_empty_gdata(num_columns);
	if (!new_gdata)
	{
	    mylog
		("unable to create %d new gdata from %d old gdata\n",
		 num_columns, self->allocated);

	    if (self->gdata)
	    {
		free(self->gdata);
		self->gdata = NULL;
	    }
	    self->allocated = 0;
	    return;
	}
	if (self->gdata)
	{
	    size_t i;

	    for (i = 0; i < self->allocated; i++)
		new_gdata[i] = self->gdata[i];
	    free(self->gdata);
	}
	self->gdata = new_gdata;
	self->allocated = num_columns;
    } else if (shrink && self->allocated > num_columns)
    {
	int i;

	for (i = self->allocated; i > num_columns; i--)
	    reset_a_getdata_info(self, i);
	self->allocated = num_columns;
	if (0 == num_columns)
	{
	    free(self->gdata);
	    self->gdata = NULL;
	}
    }

    /*
     * There is no reason to zero out extra gdata if there are more
     * than needed.  If an app has allocated extra gdata, let it worry
     * about it by unbinding those columns.
     */

    mylog("exit(%p)\n", self->gdata);
}

void reset_a_getdata_info(GetDataInfo * gdata_info, int icol)
{
    if (icol < 1 || icol > gdata_info->allocated)
	return;
    icol--;
    if (gdata_info->gdata[icol].ttlbuf)
    {
	free(gdata_info->gdata[icol].ttlbuf);
	gdata_info->gdata[icol].ttlbuf = NULL;
    }
    gdata_info->gdata[icol].ttlbuflen =
	gdata_info->gdata[icol].ttlbufused = 0;
    gdata_info->gdata[icol].data_left = -1;
}

