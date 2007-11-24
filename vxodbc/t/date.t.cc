#include "common.h"
#include "vxodbctester.h"
#include "table.h"
#include "wvtest.h"

//#define VXODBC_SUPPORTS_SQLDESCRIBECOL

static void DoTest(int convert_to_char)
{
    VxOdbcTester v;
    bool nullable = 0;
    Table t("unnamed");
    t.addCol("unnamed", ColumnInfo::DateTime, nullable, 8, 0, 0);
    // time_t value for 2002-12-27 18:43:21 UTC
    t.cols[0].append(1041014601).append(0);
    v.t = &t;
    int res;

    SQLCHAR output[256];

    SQLLEN dataSize;

    TIMESTAMP_STRUCT ts;

    v.expected_query = "select convert(datetime, '2002-12-27 18:43:21')";
    WVPASS_SQL(CommandWithResult(Statement, v.expected_query));

    WVPASS_SQL(SQLFetch(Statement));

#ifdef VXODBC_SUPPORTS_SQLDESCRIBECOL
    SQLSMALLINT colType;
    SQLULEN colSize;
    SQLSMALLINT colScale, colNullable;
    WVPASS_SQL(SQLDescribeCol(Statement, 1, output, sizeof(output), NULL, 
            &colType, &colSize, &colScale, &colNullable));
#endif

    if (convert_to_char == 0) {
        memset(&ts, 0, sizeof(ts));
        WVPASS_SQL(SQLGetData(Statement, 1, SQL_C_TIMESTAMP, &ts, sizeof(ts), 
            &dataSize));
        sprintf((char *) output, "%04d-%02d-%02d %02d:%02d:%02d", 
            ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second);
    } else {
        WVPASS_SQL(SQLGetData(Statement, 1, SQL_C_CHAR, output, sizeof(output), &dataSize));
    }

    // We add this in UTC, and get it back in EST.  Sigh.
    WVPASSEQ((char *) output, "2002-12-27 13:43:21");

    WVPASS_SQL_EQ(SQLFetch(Statement), SQL_NO_DATA);
    WVPASS_SQL(SQLCloseCursor(Statement));
}

WVTEST_MAIN("Date conversion to timestamp")
{
    DoTest(0);
}

WVTEST_MAIN("Date conversion to char")
{
    DoTest(1);
}
