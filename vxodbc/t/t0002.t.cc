#include "common.h"
#include "wvtest.h"
#include "table.h"

#include "fakeversaplex.h"

WVTEST_MAIN("Dropped statements don't destroy pending data")
{
    FakeVersaplexServer v;
    Table t("odbctestdata");
    bool nullable = 1;
    t.addCol("i", ColumnInfo::Int32, nullable, 4, 0, 0);
    v.t = &t;

    WVPASS_SQL(CommandWithResult(Statement, "drop table odbctestdata"));
    WVPASS_SQL(CommandWithResult(Statement, "create table odbctestdata (i int)"));
    WVPASS_SQL(CommandWithResult(Statement, "insert odbctestdata values (123)"));

    /*
     * now we allocate another statement, select, get all results
     * then make another query with first select and drop this statement
     * result should not disappear (required for DBD::ODBC)
     */

    HSTMT stmt;
    WvString select = "select * from odbctestdata";
    WvString empty_select = "select * from odbctestdata where 0=1";
    v.expected_query = empty_select;
    WVPASS_SQL(SQLAllocHandle(SQL_HANDLE_STMT, Connection, &stmt));
    WVPASS_SQL(CommandWithResult(stmt, empty_select.cstr()));
    WVPASS_SQL_EQ(SQLFetch(stmt), SQL_NO_DATA);
    WVPASS_SQL(SQLCloseCursor(stmt));

    v.expected_query = select;
    v.t->cols[0].append(123);
    WVPASS_SQL(CommandWithResult(Statement, select.cstr()));

    /* drop first statement .. data should not disappear */
    WVPASS_SQL(SQLFreeStmt(stmt, SQL_DROP));

    long value = 0;
    WVPASS_SQL(SQLFetch(Statement));
    WVPASS_SQL(SQLGetData(Statement, 1, SQL_C_LONG, &value, 0, NULL));
    WVPASSEQ(value, 123);
    WVPASS_SQL_EQ(SQLFetch(Statement), SQL_NO_DATA);

    WVPASS_SQL(SQLCloseCursor(Statement));
    WVPASS_SQL(CommandWithResult(Statement, "drop table odbctestdata"));
}
