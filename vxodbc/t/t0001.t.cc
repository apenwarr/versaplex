#include "wvstring.h"
#include "wvdbusserver.h"
#include "wvdbusconn.h"
#include "wvtest.h"
#include "fileutils.h"

#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "vxodbctester.h"
#include "table.h"
#include "column.h"

#include <vector>


WVTEST_MAIN("Basic data insertion and retrieval")
{
    VxOdbcTester v;

    WVPASS_SQL(CommandWithResult(Statement, "drop table odbctestdata"));

    bool nullable = 0;
    Table t("odbctestdata");
    t.addCol("col1", ColumnInfo::String, nullable, 30, 0, 0);
    t.addCol("col2", ColumnInfo::Int32, nullable, 4, 0, 0);
    t.addCol("col3", ColumnInfo::Double, nullable, 8, 0, 0);
    t.addCol("col4", ColumnInfo::Decimal, nullable, 0, 18, 6);
    t.addCol("col5", ColumnInfo::DateTime, nullable, 8, 0, 0);
    t.addCol("col6", ColumnInfo::String, nullable, 0, 0, 0);
    v.t = &t;

    // Send the CREATE TABLE statement even though we've already created it 
    // behind the scenes; this lets us also run against a real DB backend for
    // sanity checking.
    WVPASS_SQL(CommandWithResult(Statement, t.getCreateTableStmt()));

    std::vector<Column>::iterator it;
    it = t.cols.begin();
    it->append("ABCDEFGHIJKLMNOP");
    (++it)->append(123456);
    (++it)->append(1234.56);
    (++it)->append("123456.780000");
    // Let's party like it's time_t 1e9
    (++it)->append(1000000000).append(100000);
    (++it)->append("just to check returned length...");

    WVPASS(++it == t.cols.end());
    WvString command = "insert dbo.odbctestdata values ("
        "'ABCDEFGHIJKLMNOP',"
        "123456," "1234.56," "123456.78," "'Sep 9 2001 1:46:40.1AM'," 
        "'just to check returned length...')";
    WVPASS_SQL(CommandWithResult(Statement, command)); 

    v.expected_query = "select * from odbctestdata";
    WVPASS_SQL(CommandWithResult(Statement, v.expected_query));

    WVPASS_SQL(SQLFetch(Statement));

    SQLLEN cnamesize;
    SQLCHAR output[256];

    WVPASS_SQL(SQLGetData(Statement, 1, SQL_C_CHAR, 
			    output, sizeof(output), &cnamesize));

    WVFAILEQ((char *)output, WvString::null);
    WVPASSEQ((int)cnamesize, strlen((char *)output));
    WVPASSEQ((char *)output, "ABCDEFGHIJKLMNOP");
    WVPASSEQ(cnamesize, 16);

    long longval = 0;
    WVPASS_SQL(SQLGetData(Statement, 2, SQL_C_LONG, 
			    &longval, sizeof(longval), &cnamesize));
    WVPASSEQ(longval, 123456);
    WVPASSEQ(cnamesize, 4);

    double doubleval;
    WVPASS_SQL(SQLGetData(Statement, 3, SQL_C_DOUBLE, 
			    &doubleval, sizeof(doubleval), &cnamesize));
    // Sadly there's no WVPASSEQ for doubles, and it'd be difficult to add
    WVPASS(doubleval == 1234.56);
    WVPASSEQ(cnamesize, 8);

    SQL_NUMERIC_STRUCT numericval;
    WVPASS_SQL(SQLGetData(Statement, 4, SQL_C_NUMERIC, 
			    &numericval, sizeof(numericval), &cnamesize));
    WVPASSEQ(numericval.precision, 12);
    WVPASSEQ(numericval.scale, 6);
    WVPASSEQ(numericval.sign, 1);
    WVPASSEQ(numericval.val[0], (123456780000LL >> 0) % 256);
    WVPASSEQ(numericval.val[1], (123456780000LL >> 8) % 256);
    WVPASSEQ(numericval.val[2], (123456780000LL >> 16) % 256);
    WVPASSEQ(numericval.val[3], (123456780000LL >> 24) % 256);
    WVPASSEQ(numericval.val[4], (123456780000LL >> 32) % 256);
    WVPASSEQ((int)cnamesize, sizeof(numericval));

    // We added this in GMT, but get it back in local time.  This means that
    // this code probably fails outside of the Eastern timezone.  Sigh.
    SQL_TIMESTAMP_STRUCT ts;
    WVPASS_SQL(SQLGetData(Statement, 5, SQL_C_TIMESTAMP, 
			    &ts, sizeof(ts), &cnamesize));
    WVPASSEQ(ts.year, 2001);
    WVPASSEQ(ts.month, 9);
    WVPASSEQ(ts.day, 8);
    WVPASSEQ(ts.hour, 20);
    WVPASSEQ(ts.minute, 46);
    WVPASSEQ(ts.second, 40);
    WVPASSEQ(ts.fraction, 100000000);
    WVPASSEQ((int)cnamesize, sizeof(ts));

    WVPASS_SQL(SQLGetData(Statement, 6, SQL_C_CHAR, 
			    output, sizeof(output), &cnamesize));

    WVFAILEQ((char *)output, WvString::null);
    WVPASSEQ((int)cnamesize, strlen((char *)output));
    WVPASSEQ((char *)output, "just to check returned length...");

    WVPASS_SQL_EQ(SQLFetch(Statement), SQL_NO_DATA);
    WVPASS_SQL(SQLCloseCursor(Statement));

    WVPASS_SQL(CommandWithResult(Statement, "drop table odbctestdata"));
}
