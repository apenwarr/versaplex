#ifndef __VXHELPERS_H
#define __VXHELPERS_H

#include <wvdbusconn.h>
#include "pgtypes.h"


class VxResultSet
{
    int maxcol;

    int vxtype_to_pgtype(WvStringParm vxtype)
    {
	if (vxtype == "String")
	    return PG_TYPE_VARCHAR;
	else if (vxtype == "Int64")
	    return PG_TYPE_INT8;
	else if (vxtype == "Int32")
	    return PG_TYPE_INT4;
	else if (vxtype == "Int16")
	    return PG_TYPE_INT2;
	else if (vxtype == "UInt8")
	    return PG_TYPE_CHAR;
	else if (vxtype == "Bool")
	    return PG_TYPE_BOOL;
	else if (vxtype == "Double")
	    return PG_TYPE_FLOAT8;
	else if (vxtype == "Uuid")
	    return PG_TYPE_VARCHAR;
	else if (vxtype == "Binary")
	    return PG_TYPE_BYTEA;
	else if (vxtype == "DateTime")
	    return VX_TYPE_DATETIME;
	else if (vxtype == "Decimal")
	    return PG_TYPE_NUMERIC;
    }
public:
    QResultClass *res;
    
    VxResultSet()
    {
	res = QR_Constructor();
	maxcol = -1;
	assert(res);
	assert(*this == res);
    }
    
    void set_field_info(int col, const char *colname, OID type, int typesize)
    {
	mylog("Col#%d is '%s', type=%d, size=%d\n", col, colname, type, typesize);
	if (maxcol < col)
	{
	    // FIXME: This will destroy old data
	    mylog("!!!!!! Resizing colinfo array, destroying data !!!!!!\n");
	    maxcol = col;
	    QR_set_num_fields(res, maxcol+1);
	}
	QR_set_field_info_v(res, col, colname, type, typesize);
	assert(!strcmp(QR_get_fieldname(res, col), colname));
    }
    
    operator QResultClass* () const
    {
	return res;
    }
    
    int numcols() const
    {
	return maxcol+1;
    }
    
    void runquery(WvDBusConn &conn, const char *func, const char *query)
    {
	WvDBusMsg msg("com.versabanq.versaplex",
		      "/com/versabanq/versaplex/db",
		      "com.versabanq.versaplex.db",
		      func);
	msg.append(query);
	WvDBusMsg reply = conn.send_and_wait(msg, 50000);
	
	if (reply.iserror())
	    mylog("DBus error: '%s'\n", ((WvString)reply).cstr());
	else
	{
	    WvDBusMsg::Iter top(reply);
	    WvDBusMsg::Iter colinfo(top.getnext().open());
	    WvDBusMsg::Iter data(top.getnext().open().getnext().open());
	    WvDBusMsg::Iter flags(top.getnext().open());

	    int num_cols;
	    // Sadly there's no better way to get the number of columns
	    for (num_cols = 0; colinfo.next(); num_cols++) { }
	    colinfo.rewind();
	    // Allocate space for the column info data.
	    QR_set_num_fields(res, num_cols);
	    maxcol = num_cols - 1;
	    
	    for (int colnum = 0; colinfo.next(); colnum++)
	    {
		WvDBusMsg::Iter i(colinfo.open());

		int colsize = i.getnext();
		WvString colname = i.getnext();
		int coltype = vxtype_to_pgtype(i.getnext());

		set_field_info(colnum, colname, coltype, colsize);
	    }
	    
	    for (data.rewind(); data.next(); )
	    {
		TupleField *tuple = QR_AddNew(res);
		
		WvDBusMsg::Iter cols(data.open());
		for (int colnum = 0;
		       cols.next() && colnum < numcols();
		       colnum++)
		    set_tuplefield_string(&tuple[colnum], *cols);
	    }
	}
    }
};


class VxStatement
{
    StatementClass *stmt;
    RETCODE ret;
public:
    VxStatement(StatementClass *_stmt)
    {
	stmt = _stmt;
	ret = SQL_SUCCESS;
    }
    
    ~VxStatement()
    {
	/*
	 * things need to think that this statement is finished so the
	 * results can be retrieved.
	 */
	stmt->status = STMT_FINISHED;
	
	/* set up the current tuple pointer for SQLFetch */
	stmt->currTuple = -1;
	SC_set_rowset_start(stmt, -1, FALSE);
	SC_set_current_col(stmt, -1);
	
	if (stmt->internal)
	    ret = DiscardStatementSvp(stmt, ret, FALSE);
    }
    
    void reinit()
    {
	ret = SC_initialize_and_recycle(stmt);
    }
    
    void set_result(VxResultSet &rs)
    {
	SC_set_Result(stmt, rs);
	
	/* the binding structure for a statement is not set up until
	 * a statement is actually executed, so we'll have to do this
	 * ourselves.
	 */
	extend_column_bindings(SC_get_ARDF(stmt), rs.numcols());
    }
    
    RETCODE retcode() const
    {
	return ret;
    }
    
    void seterr()
    {
	ret = SQL_ERROR;
    }
    
    void setok()
    {
	ret = SQL_SUCCESS;
    }
    
    bool isok() const
    {
	return ret == SQL_SUCCESS;
    }
    
    WvDBusConn &dbus()
    {
	WvDBusConn *db = SC_get_conn(stmt)->dbus;
	assert(db);
	return *db;
    }
};

#endif // __VXHELPERS_H
