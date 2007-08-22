#ifndef __VXHELPERS_H
#define __VXHELPERS_H

#include <wvdbusconn.h>


class VxResultSet
{
    int maxcol;
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
	mylog("Col#%d is '%s'\n", col, colname);
	if (maxcol < col)
	{
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
	WvDBusMsg reply = conn.send_and_wait(msg, 5000);
	
	if (reply.iserror())
	    mylog("DBus error: '%s'\n", ((WvString)reply).cstr());
	else
	{
	    WvDBusMsg::Iter top(reply);
	    WvDBusMsg::Iter colnames(top.getnext().open());
	    WvDBusMsg::Iter coltypes(top.getnext().open());
	    WvDBusMsg::Iter data(top.getnext().open().getnext().open());
	    WvDBusMsg::Iter flags(top.getnext().open());
	    
	    for (int colnum = 0; colnames.next(); colnum++)
		set_field_info(colnum, *colnames,
			       PG_TYPE_VARCHAR, MAX_INFO_STRING);
	    
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
	ret = SC_initialize_and_recycle(stmt);
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
