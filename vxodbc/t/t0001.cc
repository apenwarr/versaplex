#include "wvstring.h"
#include "wvdbusserver.h"
#include "wvdbusconn.h"
#include "wvtest.h"
#include "fileutils.h"

#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "fakeversaplex.h"
#include "table.h"
#include "column.h"

#include <vector>


int main(int argc, char *argv[])
{
    FakeVersaplexServer v;
    WvString command;

    Connect();

    WVPASS_SQL(CommandWithResult(Statement, "drop table odbctestdata"));

    bool not_null = 0;
    Table t("odbctestdata");
    t.addCol("col1", ColumnInfo::String, not_null, 30, 0, 0);
    t.addCol("col2", ColumnInfo::Int32, not_null, 4, 0, 0);
    t.addCol("col3", ColumnInfo::Double, not_null, 8, 0, 0);
    t.addCol("col4", ColumnInfo::Decimal, not_null, 0, 18, 6);
    t.addCol("col5", ColumnInfo::DateTime, not_null, 8, 0, 0);
    t.addCol("col6", ColumnInfo::String, not_null, 0, 0, 0);
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
    (++it)->append("123456.78");
    // Note: this doesn't match the value inserted, but nobody actually checks.
    (++it)->append(123456).append(0);
    (++it)->append("just to check returned length...");

    WVPASS(it == t.cols.end());
    command = "insert dbo.odbctestdata values ("
        "'ABCDEFGHIJKLMNOP',"
        "123456," "1234.56," "123456.78," "'Sep 11 2001 10:00AM'," 
        "'just to check returned length...')";
    WVPASS_SQL(CommandWithResult(Statement, command)); 

    v.expected_query = "select * from odbctestdata";
    WVPASS_SQL(CommandWithResult(Statement, v.expected_query));

    WVPASS_SQL(SQLFetch(Statement));

    for (int i = 1; i <= 6; i++) {
        SQLLEN cnamesize;
        SQLCHAR output[256];

        WVPASS_SQL(SQLGetData(Statement, i, SQL_C_CHAR, 
                                output, sizeof(output), &cnamesize));

        WVFAILEQ((char *)output, WvString::null);
        WVPASSEQ((int)cnamesize, strlen((char *)output));
    }

    WVPASS_SQL_EQ(SQLFetch(Statement), SQL_NO_DATA);
    WVPASS_SQL(SQLCloseCursor(Statement));

    WVPASS_SQL(CommandWithResult(Statement, "drop table odbctestdata"));

    Disconnect();

    printf("Done.\n");
    return 0;
}
