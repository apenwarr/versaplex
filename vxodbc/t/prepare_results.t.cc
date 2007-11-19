#include "common.h"
#include "wvtest.h"
#include "fakeversaplex.h"
#include "table.h"

/* Test for data format returned from SQLPrepare */

WVTEST_MAIN("SQLPrepare results")
{
    FakeVersaplexServer v;
    Table t("odbctestdata");
    bool nullable = 1;
    t.addCol("i", ColumnInfo::Int32, nullable, 4, 0, 0);
    t.addCol("c", ColumnInfo::String, nullable, 20, 0, 0);
    t.addCol("n", ColumnInfo::Decimal, nullable, 17, 34, 12);
    v.t = &t;

    v.expected_query = 
        "create table odbctestdata (i int, c char(20), n numeric(34,12) )";
    WVPASS_SQL(CommandWithResult(Statement, v.expected_query));

    /* reset state */
    v.expected_query = "select * from odbctestdata";
    WVPASS_SQL(CommandWithResult(Statement, v.expected_query.cstr()));
    SQLFetch(Statement);
    SQLMoreResults(Statement);

    /* test query returns column information for update */
    v.expected_query = "update odbctestdata set i = 20";
#ifdef VXODBC_SUPPORTS_SQLPREPARE
    WVPASS_SQL(SQLPrepare(Statement, (SQLCHAR *)v.expected_query.cstr(), SQL_NTS));
#else
    WVPASS_SQL(CommandWithResult(Statement, v.expected_query.cstr()));
#endif

    SQLSMALLINT count = 0; 
#ifdef VXODBC_SUPPORTS_EMPTY_RESULTSETS
    WVPASS_SQL(SQLNumResultCols(Statement, &count));
    WVPASSEQ(count, 0);
#endif

    /* test query returns column information */
    v.expected_query = "select * from odbctestdata";
#ifdef VXODBC_SUPPORTS_SQLPREPARE
    WVPASS_SQL(SQLPrepare(Statement, (SQLCHAR *)v.expected_query.cstr(), SQL_NTS));
#else
    WVPASS_SQL(CommandWithResult(Statement, v.expected_query.cstr()));
#endif

    WVPASS_SQL(SQLNumResultCols(Statement, &count));
    WVPASSEQ(count, 3);

#ifdef VXODBC_SUPPORTS_SQLDESCRIBECOL
    SQLSMALLINT namelen = 0, type = 0, digits = 0, nullable = 0;
    SQLULEN size = 0;
    char name[128] = { 0 };

    WVPASS_SQL(SQLDescribeCol(Statement, 1, (SQLCHAR *) name, sizeof(name), 
            &namelen, &type, &size, &digits, &nullable));

    WVPASSEQ(type, SQL_INTEGER);
    WVPASSEQ(name, "i");

    WVPASS_SQL(SQLDescribeCol(Statement, 2, (SQLCHAR *) name, sizeof(name), 
            &namelen, &type, &size, &digits, &nullable));

    WVPASSEQ(type, SQL_CHAR);
    WVPASSEQ(name, "c"); 
    WVPASSEQ(size, 20);

    WVPASS_SQL(SQLDescribeCol(Statement, 3, (SQLCHAR *) name, sizeof(name), 
            &namelen, &type, &size, &digits, &nullable));

    WVPASSEQ(type, SQL_NUMERIC);
    WVPASSEQ(name, "n"); 
    WVPASSEQ(size, 34); 
    WVPASSEQ(digits, 12);
#endif

    /* TODO test SQLDescribeParam (when implemented) */

    // Disable so it's easier to run against the real Versaplex (which doesn't 
    // support CREATE TABLE just yet).
#if VERSAPLEX_SUPPORTS_CREATE_TABLE
    Command(Statement, "drop table odbctestdata");
#endif
}
