#include "common.h"
#include "wvtest.h"
#include "fakeversaplex.h"

/* Test for {?=call store(?,123,'foo')} syntax and run */

WVTEST_MAIN("Stored procedures with const parameters")
{
    FakeVersaplexServer v;
    SQLINTEGER input, ind, ind2, ind3, output;
    SQLINTEGER out1;

    Connect();

    WVPASS_SQL(CommandWithResult(Statement, "drop proc const_param"));

    WVPASS_SQL(Command(Statement, 
        "create proc const_param \n"
        "@in1 int, @in2 int, @in3 datetime, @in4 varchar(10), "
        "@out int output as\n"
        "begin\n"
        " set nocount on\n"
        " select @out = 7654321\n"
        " if (@in1 <> @in2 and @in2 is not null) "
                " or @in3 <> convert(datetime, '2004-10-15 12:09:08')"
                " or @in4 <> 'foo'\n"
        "  select @out = 1234567\n"
        " return 24680\n"
        "end"));

    WVPASS_SQL(SQLBindParameter(Statement, 1, SQL_PARAM_INPUT, SQL_C_SLONG, 
            SQL_INTEGER, 0, 0, &input, 0, &ind));

    WVPASS_SQL(SQLBindParameter(Statement, 2, SQL_PARAM_OUTPUT, SQL_C_SLONG, 
            SQL_INTEGER, 0, 0, &out1, 0, &ind2));

    /* TODO use {ts ...} for date */
    WVPASS_SQL(SQLPrepare(Statement, (SQLCHAR *) 
            "call const_param(?, 13579, '2004-10-15 12:09:08', 'foo', ?)", 
            SQL_NTS));

    input = 13579;
    ind = sizeof(input);
    out1 = output = 0xdeadbeef;
    WVPASS_SQL(SQLExecute(Statement));

#if VXODBC_SUPPORTS_BOUND_PARAMETERS
    WVPASSEQ(out1, 7654321);
#endif

    /* just to reset some possible buffers */
    WVPASS_SQL(Command(Statement, "DECLARE @i INT"));

    WVPASS_SQL(SQLBindParameter(Statement, 1, SQL_PARAM_OUTPUT, SQL_C_SLONG, 
            SQL_INTEGER, 0, 0, &output, 0, &ind));
    WVPASS_SQL(SQLBindParameter(Statement, 2, SQL_PARAM_INPUT, SQL_C_SLONG, 
            SQL_INTEGER, 0, 0, &input, 0, &ind2));
    WVPASS_SQL(SQLBindParameter(Statement, 3, SQL_PARAM_OUTPUT, SQL_C_SLONG, 
            SQL_INTEGER, 0, 0, &out1, 0, &ind3));

    /* TODO use {ts ...} for date */
    WVPASS_SQL(SQLPrepare(Statement, (SQLCHAR *) 
            "{?=call const_param(?, , '2004-10-15 12:09:08', 'foo', ?)}", 
            SQL_NTS));

    input = 13579;
    ind2 = sizeof(input);
    out1 = output = 0xdeadbeef;
    WVPASS_SQL(SQLExecute(Statement));

#if VXODBC_SUPPORTS_BOUND_PARAMETERS
    WVPASSEQ(out1, 7654321);
    WVPASSEQ(output, 24680);
#endif

    WVPASS_SQL(CommandWithResult(Statement, "drop proc const_param"));

    WVPASS_SQL(Command(Statement, 
        "create proc const_param @in1 float, @in2 varbinary(100) as\n"
        "begin\n"
        " if @in1 <> 12.5 or @in2 <> 0x0102030405060708\n"
        "  return 12345\n"
        " return 54321\n"
        "end"));

    WVPASS_SQL(SQLBindParameter(Statement, 1, SQL_PARAM_OUTPUT, SQL_C_SLONG, 
            SQL_INTEGER, 0, 0, &output, 0, &ind));

    WVPASS_SQL(SQLPrepare(Statement, (SQLCHAR *) 
            "{?=call const_param(12.5, 0x0102030405060708)}", SQL_NTS));

    output = 0xdeadbeef;
    WVPASS_SQL(SQLExecute(Statement));
#if VXODBC_SUPPORTS_BOUND_PARAMETERS
    WVPASSEQ(output, 54321);
#endif

    WVPASS_SQL(CommandWithResult(Statement, "drop proc const_param"));

    WVPASS_SQL(Command(Statement, 
        "create proc const_param @in varchar(20) as\n"
        "begin\n"
        " if @in = 'value' select 8421\n"
        " select 1248\n"
        "end"));

    /* catch problem reported by Peter Deacon */
    output = 0xdeadbeef;
    WVPASS_SQL(Command(Statement, "{CALL const_param('value')}"));
    WVPASS_SQL(SQLBindCol(Statement, 1, SQL_C_SLONG, &output, 0, &ind));
#if VXODBC_SUPPORTS_BOUND_PARAMETERS
    WVPASS_SQL(SQLFetch(Statement));

    WVPASSEQ(output, 8421);
#endif

    WVPASS_SQL(CommandWithResult(Statement, "drop proc const_param"));

    Disconnect();
}
