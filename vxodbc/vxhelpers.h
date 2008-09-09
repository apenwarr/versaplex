#ifndef __VXHELPERS_H
#define __VXHELPERS_H

#include "statement.h"
#include "qresult.h"
#include <wvdbusconn.h>
#include "pgtypes.h"


class VxResultSet
{
    int maxcol;

    //A bool to select whether or not the column info from an incoming
    //message is important or not.
    bool process_colinfo;

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
    
    VxResultSet() : process_colinfo(true)
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

    void runquery(WvDBusConn &conn, const char *func, const char *query);
    void process_msg(WvDBusMsg &msg);
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
