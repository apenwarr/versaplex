#include "common.h"
#include "wvtest.h"
#include "table.h"
#include "fakeversaplex.h"

WVTEST_MAIN("SQLGetData")
{
    FakeVersaplexServer v;
    bool nullable = 1;
    Table t("whatever");
    t.addCol("", ColumnInfo::String, nullable, 0, 0, 0);
    t.cols[0].append("Prova");
    v.t = &t;
    char buf[16];

    /* TODO test with VARCHAR too */
    v.expected_query = "SELECT CONVERT(TEXT,'Prova')";
    WVPASS_SQL(Command(Statement, v.expected_query.cstr()));

    WVPASS_SQL(SQLFetch(Statement));

    /* these 2 tests test an old severe BUG in FreeTDS */
    WVPASS_SQL_EQ(SQLGetData(Statement, 1, SQL_C_CHAR, buf, 0, NULL), 
            SQL_SUCCESS_WITH_INFO);

    WVPASS_SQL_EQ(SQLGetData(Statement, 1, SQL_C_CHAR, buf, 0, NULL), 
            SQL_SUCCESS_WITH_INFO);

    WVPASS_SQL_EQ(SQLGetData(Statement, 1, SQL_C_CHAR, buf, 3, NULL), 
            SQL_SUCCESS_WITH_INFO);
    WVPASSEQ(buf, "Pr");

    WVPASS_SQL(SQLGetData(Statement, 1, SQL_C_CHAR, buf, 16, NULL));
    WVPASSEQ(buf, "ova");
}
