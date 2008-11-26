#include "common.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "wvtest.h"
#include "vxodbctester.h"
#include "table.h"

/* Test various bind type */

static int result = 0;
static char sbuf[1024];

//#define VXODBC_SUPPORTS_CONVERTING_BINARY
//#define VXODBC_SUPPORTS_CONVERTING_DATETIME_TO_BINARY
//#define VXODBC_SUPPORTS_CONVERTING_INTS_TO_BINARY
//#define VXODBC_SUPPORTS_CONVERTING_DECIMALS_TO_BINARY
//#define VXODBC_SUPPORTS_CONVERTING_FIXED_LENTGH_COLUMNS

static void Test(VxOdbcTester &v, 
                const char *type, const char *value_to_convert, 
                SQLSMALLINT out_c_type, const char *expected)
{
    unsigned char out_buf[256];
    SQLLEN out_len = 0;
    SQL_NUMERIC_STRUCT *num;
    int i;

    SQLFreeStmt(Statement, SQL_UNBIND);
    SQLFreeStmt(Statement, SQL_RESET_PARAMS);

    /* execute a select to get data as wire */
    sprintf(sbuf, "SELECT CONVERT(%s, '%s') AS data", type, value_to_convert);
    v.expected_query = sbuf;
    Command(Statement, sbuf);
    SQLBindCol(Statement, 1, out_c_type, out_buf, sizeof(out_buf), &out_len);
    WVPASS_SQL(SQLFetch(Statement));
    WVPASS_SQL_EQ(SQLFetch(Statement), SQL_NO_DATA);
    WVPASS_SQL_EQ(SQLMoreResults(Statement), SQL_NO_DATA);

    /* test results */
    sbuf[0] = 0;
    switch (out_c_type) {
    case SQL_C_NUMERIC:
        num = (SQL_NUMERIC_STRUCT *) out_buf;
        sprintf(sbuf, "%d %d %d ", num->precision, num->scale, num->sign);
        i = SQL_MAX_NUMERIC_LEN;
        for (; i > 0 && !num->val[--i];);
        for (; i >= 0; --i)
            sprintf(strchr(sbuf, 0), "%02X", num->val[i]);
        break;
    case SQL_C_BINARY:
        assert(out_len >= 0);
        for (i = 0; i < out_len; ++i)
            sprintf(strchr(sbuf, 0), "%02X", (int) out_buf[i]);
        break;
    default:
        /* not supported */
        WVPASSEQ("Unexpected out_c_type value", WvString(out_c_type));
        break;
    }

    WVPASSEQ(sbuf, expected);
    if (strcmp(sbuf, expected) != 0) {
        fprintf(stderr, "Wrong result\n  Got: %s\n  Expected: %s\n", sbuf, expected);
        result = 1;
    }
}

WVTEST_MAIN("Data conversion")
{
    VxOdbcTester v;
    Table t("conversions");
    v.t = &t;

    int big_endian = 1;
    char version[32];
    SQLSMALLINT version_len;

    if (((char *) &big_endian)[0] == 1)
        big_endian = 0;
    memset(version, 0, sizeof(version));
    SQLGetInfo(Connection, SQL_DBMS_VER, version, sizeof(version), &version_len);

    // FIXME: Strictly, these columns should be nullable, but ATM saying that
    // also makes the fake Versaplex server say that the data *is* null.
    bool nullable = 0;
    t.addCol("data", ColumnInfo::Decimal, nullable, 17, 18, 2).append("123.00");
    Test(v, "NUMERIC(18,2)", "123", SQL_C_NUMERIC, "5 2 1 300C");

    /* all binary results */
    t.cols.clear();
    t.addCol("data", ColumnInfo::String, nullable, 17, 0, 0);
#ifdef VXODBC_SUPPORTS_CONVERTING_FIXED_LENTGH_COLUMNS
    t.cols[0].append("pippo");
    Test(v, "CHAR(7)", "pippo", SQL_C_BINARY, "706970706F2020");
    t.cols[0].data.clear();
#endif
    t.cols[0].append("mickey");
    Test(v, "TEXT", "mickey", SQL_C_BINARY, "6D69636B6579");
    t.cols[0].zapData().append("foo");
    Test(v, "VARCHAR(20)", "foo", SQL_C_BINARY, "666F6F");

#ifdef VXODBC_SUPPORTS_CONVERTING_BINARY
    t.cols.clear();
    t.addCol("data", ColumnInfo::Binary, nullable, 5, 0, 0).append("qwer");
    Test(v, "BINARY(5)", "qwer", SQL_C_BINARY, "7177657200");
    t.cols.clear();
    t.addCol("data", ColumnInfo::Binary, nullable, 10, 0, 0).append("cricetone");
    Test(v, "IMAGE", "cricetone", SQL_C_BINARY, "6372696365746F6E65");
    t.cols.clear();
    t.addCol("data", ColumnInfo::Binary, nullable, 3, 0, 0).append("teo");
    Test(v, "VARBINARY(20)", "teo", SQL_C_BINARY, "74656F");
#endif

    // This checks that a TIMESTAMP value is truncated to 8 bytes
    t.cols.clear();
    t.addCol("data", ColumnInfo::Binary, nullable, 8, 0, 0).append("abcdefghi");
    // This insanely ugly mess is "[y97,y98,y99,y100,y101,y102,y103,y104]" 
    // rendered into its component bytes.  The important thing is that it
    // only represents the first 8 bytes.
    Test(v, "TIMESTAMP", "abcdefghi", SQL_C_BINARY, 
	//[ 9 7 , 9 8 , 9 9 , 1 0 0 , 1 0 1 ,
	"5B39372C39382C39392C3130302C3130312C"
	//1 0 2 , 1 0 3 , 1 0 4 ]
	"3130322C3130332C3130345D");

#ifdef VXODBC_SUPPORTS_CONVERTING_DATETIME_TO_BINARY
    t.cols.clear();
    t.addCol("data", ColumnInfo::DateTime, nullable, 8, 0, 0);
    // FIXME: This value probably isn't right
    t.cols[0].append(0x00009497).append(0x00FBAA2C);
    Test(v, "DATETIME", "2004-02-24 15:16:17", SQL_C_BINARY, 
        big_endian ? "0000949700FBAA2C" : "979400002CAAFB00");
    Test(v, "SMALLDATETIME", "2004-02-24 15:16:17", SQL_C_BINARY, 
        big_endian ? "94970394" : "97949403");
#endif

#ifdef VXODBC_SUPPORTS_CONVERTING_INTS_TO_BINARY
    t.cols.clear();
    t.addCol("data", ColumnInfo::Bool, nullable, 1, 0, 0).append(1);
    Test(v, "BIT", "1", SQL_C_BINARY, "01");
    t.cols[0].zapData().append(0);
    Test(v, "BIT", "0", SQL_C_BINARY, "00");
    t.cols.clear();
    t.addCol("data", ColumnInfo::UInt8, nullable, 1, 0, 0).append(231);
    Test(v, "TINYINT", "231", SQL_C_BINARY, "E7");
    t.cols.clear();
    t.addCol("data", ColumnInfo::Int16, nullable, 1, 0, 0).append(4231);
    Test(v, "SMALLINT", "4321", SQL_C_BINARY, big_endian ? "10E1" : "E110");
    t.cols.clear();
    t.addCol("data", ColumnInfo::Int32, nullable, 1, 0, 0).append(1234567);
    Test(v, "INT", "1234567", SQL_C_BINARY, big_endian ? "0012D687" : "87D61200");
    //
    // FIXME: This doesn't really make any sense for VxODBC.  Test what's
    // supposed to work, or kill it.
    /* TODO some Sybase versions */
    if (db_is_microsoft() && strncmp(version, "08.00.", 6) == 0) {
        int old_result = result;

        t.cols.clear();
        t.addCol("data", ColumnInfo::Int64, nullable, 1, 0, 0).append(123456789012345LL);
        Test(v, "BIGINT", "123456789012345", SQL_C_BINARY, big_endian ? "00007048860DDF79" : "79DF0D8648700000");
        if (result && strcmp(sbuf, "13000179DF0D86487000000000000000000000") == 0) {
            fprintf(stderr, "Ignore previous error. You should configure TDS 8.0 for this!!!\n");
            if (!old_result)
                result = 0;
        }
    }
#endif

#ifdef VXODBC_SUPPORTS_CONVERTING_DECIMALS_TO_BINARY
    t.cols.clear();
    t.addCol("data", ColumnInfo::Decimal, nullable, 17, 8, 4).append("1234.5678");
    Test(v, "DECIMAL", "1234.5678", SQL_C_BINARY, "120001D3040000000000000000000000000000");
    t.cols[0].zapData().append("8765.4321");
    Test(v, "NUMERIC", "8765.4321", SQL_C_BINARY, "1200013D220000000000000000000000000000");

    t.cols.clear();
    t.addCol("data", ColumnInfo::Double, nullable, 17, 8, 4).append(1234.5678);
    Test(v, "FLOAT", "1234.5678", SQL_C_BINARY, big_endian ? "40934A456D5CFAAD" : "ADFA5C6D454A9340");
    t.cols[0].zapData().append(8765.4321);
    Test(v, "REAL", "8765.4321", SQL_C_BINARY, big_endian ? "4608F5BA" : "BAF50846");

    t.cols[0].zapData().append(765.4321);
    Test(v, "SMALLMONEY", "765.4321", SQL_C_BINARY, big_endian ? "0074CBB1" : "B1CB7400");
    t.cols[0].zapData().append(4321234.5678);
    Test(v, "MONEY", "4321234.5678", SQL_C_BINARY, big_endian ? "0000000A0FA8114E" : "0A0000004E11A80F");
#endif

#if VXODBC_REALLY_WANTS_TO_EMULATE_MSSQL
    /* behavior is different from MS ODBC */
    if (db_is_microsoft() && !driver_is_freetds()) {
        Test(v, "NCHAR(7)", "donald", SQL_C_BINARY, "64006F006E0061006C0064002000");
        Test(v, "NTEXT", "duck", SQL_C_BINARY, "6400750063006B00");
        Test(v, "NVARCHAR(20)", "daffy", SQL_C_BINARY, "64006100660066007900");
    }

    if (db_is_microsoft())
        Test(v, "UNIQUEIDENTIFIER", "0DDF3B64-E692-11D1-AB06-00AA00BDD685", SQL_C_BINARY,
             big_endian ? "0DDF3B64E69211D1AB0600AA00BDD685" : "643BDF0D92E6D111AB0600AA00BDD685");
#endif
}
