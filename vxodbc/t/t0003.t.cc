#include "common.h"
#include "fakeversaplex.h"
#include "table.h"
#include "wvtest.h"

/* Test for SQLMoreResults */

static void
DoTest(int prepared)
{
    FakeVersaplexServer v;
    Table t("odbctestdata");
    bool null = 1;
    t.addCol("i", ColumnInfo::Int32, null, 4, 0, 0);
    v.t = &t;
    Command(Statement, "create table odbctestdata (i int)");

    /* test that 2 empty result set are returned correctly */
    v.expected_query =
        "select * from odbctestdata select * from odbctestdata"; 
    if (!prepared) {
        WVPASS_SQL(Command(Statement, v.expected_query.cstr()));
    } else {
        WVPASS_SQL(SQLPrepare(Statement, (SQLCHAR *)v.expected_query.cstr(), 
            SQL_NTS));
        WVPASS_SQL(SQLExecute(Statement));
    }

    WVPASS_SQL_EQ(SQLFetch(Statement), SQL_NO_DATA);
    WVPASS_SQL(SQLMoreResults(Statement));

    printf("Getting next recordset\n");

    WVPASS_SQL_EQ(SQLFetch(Statement), SQL_NO_DATA);
    WVPASS_SQL_EQ(SQLMoreResults(Statement), SQL_NO_DATA);

    /* test that skipping a no empty result go to other result set */
    WVPASS_SQL(Command(Statement, "insert into odbctestdata values(123)"));
    if (!prepared) {
        WVPASS_SQL(Command(Statement, 
            "select * from odbctestdata select * from odbctestdata"));
    } else {
        WVPASS_SQL(SQLPrepare(Statement, (SQLCHAR *)
            "select * from odbctestdata select * from odbctestdata", SQL_NTS));
        WVPASS_SQL(SQLExecute(Statement));
    }

    WVPASS_SQL(SQLMoreResults(Statement));
    printf("Getting next recordset\n");

    WVPASS(SQLFetch(Statement));

    WVPASS_SQL_EQ(SQLFetch(Statement), SQL_NO_DATA);
    WVPASS_SQL_EQ(SQLMoreResults(Statement), SQL_NO_DATA);

    WVPASS_SQL(Command(Statement, "drop table odbctestdata"));
}

WVTEST_MAIN("Two empty record sets, unprepared")
{
    Connect();
    DoTest(0);
    Disconnect();
}

WVTEST_MAIN("Two empty record sets, prepared")
{
    Connect();
    DoTest(0);
    Disconnect();
}
