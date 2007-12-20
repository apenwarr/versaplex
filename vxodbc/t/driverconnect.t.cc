#include "vxodbctester.h"
#include "wvtest.h"
#include "common.h"
#include "table.h"
#include "column.h"

void sanity_check(VxOdbcTester &v)
{
    WVPASS_SQL(CommandWithResult(Statement, "drop table odbctestdata"));

    bool nullable = false;
    Table t("odbctestdata");
    v.t = &t;
    t.addCol("col1", ColumnInfo::Int32, nullable, 4, 0, 0).append(123456);
    WVPASS_SQL(CommandWithResult(Statement, t.getCreateTableStmt()));

    WvString command = "insert dbo.odbctestdata values (123456)";
    WVPASS_SQL(CommandWithResult(Statement, command));

    v.expected_query = "select * from odbctestdata";
    WVPASS_SQL(CommandWithResult(Statement, v.expected_query));

    WVPASS_SQL(SQLFetch(Statement));
    long longval = 0;
    long cnamesize;
    WVPASS_SQL(SQLGetData(Statement, 1, SQL_C_LONG, 
			    &longval, sizeof(longval), &cnamesize));
    WVPASSEQ(longval, 123456);

    WVPASSEQ(SQLFetch(Statement), SQL_NO_DATA);
    WVPASS_SQL(SQLCloseCursor(Statement));

    WVPASS_SQL(CommandWithResult(Statement, "drop table odbctestdata"));
}

WVTEST_MAIN("SQLDriverConnect with DBus moniker")
{
    VxOdbcTester v;

    // FIXME: This uses the disgusting global variables in common.cc, and
    // mucks about undoing and redoing some of the same the stuff that
    // Connect() does (called from the VxOdbcTester constructor).
    SQLFreeStmt(Statement, SQL_DROP);
    SQLDisconnect(Connection);
    Statement = SQL_NULL_HSTMT;

    // FIXME: Some of this data isn't really required
    // FIXME: Should also test connecting using server=blah;port=blah
    WvString connstr("DRIVER=vxodbc;UID=pmccurdy;PWD=scs;database=pmccurdy;"
        "DBus=%s", v.dbus_moniker);
    SQLCHAR outbuf[1024];
    SQLSMALLINT num_written = 0;
    WVPASS_SQL(SQLDriverConnect(Connection, NULL, 
        (SQLCHAR*)connstr.cstr(), connstr.len(), 
        outbuf, sizeof(outbuf), &num_written, SQL_DRIVER_NOPROMPT));

    // Now check that the connection is working.
    WVPASS_SQL(SQLAllocHandle(SQL_HANDLE_STMT, Connection, &Statement));

    sanity_check(v);
}

WVTEST_MAIN("SQLDriverConnect server and port")
{
    // Be sure to create a TCP DBus server, as we obviously can't test the
    // server and port settings if we have to use dbus:session
    VxOdbcTester v(true);
    int port;
    WvString host;
    WvStringList hostdata;
    hostdata.split(v.dbus_server.s->get_addr(), ":");
    WVPASSEQ(hostdata.popstr(), "tcp");
    host = hostdata.popstr();
    port = hostdata.popstr().num();
    if (host == "0.0.0.0")
        host = "127.0.0.1";
    WVFAILEQ(WvString(host), WvString(port));

    SQLFreeStmt(Statement, SQL_DROP);
    SQLDisconnect(Connection);
    Statement = SQL_NULL_HSTMT;

    WvString connstr("DRIVER=vxodbc;UID=pmccurdy;PWD=scs;database=pmccurdy;"
        "Server=%s;Port=%s", host, port);

    SQLCHAR outbuf[1024];
    SQLSMALLINT num_written = 0;
    WVPASS_SQL(SQLDriverConnect(Connection, NULL, 
        (SQLCHAR*)connstr.cstr(), connstr.len(), 
        outbuf, sizeof(outbuf), &num_written, SQL_DRIVER_NOPROMPT));

    // Now check that the connection is working.
    WVPASS_SQL(SQLAllocHandle(SQL_HANDLE_STMT, Connection, &Statement));

    sanity_check(v);
}

