#include "wvtest.cs.h"

using Wv;
using Wv.Test;

[TestFixture]
class QuerySecurityTests : VersaplexTester
{
    private WvDbusMsg setup_msg(string query)
    {
	WvDbusMsg msg = methodcall("ExecChunkRecordset", "s");

	WvDbusWriter mw = new WvDbusWriter();
	mw.Write(query);

	msg.Body = mw.ToArray();

	return msg;
    }

    private WvDbusMsg fire_and_retrieve(string query)
    {
	return bus.send_and_wait(setup_msg(query));
    }
   
    static string failmsg = "Nice try, sneaking multiple requests in there!";

    private void confirmpass(WvDbusMsg response)
    {
	WVPASS(response.type == Wv.Dbus.MType.MethodReturn);
    }

    private void confirmfail(WvDbusMsg response)
    {
	WVPASS(response.type = Wv.Dbus.MType.Error);
	WVPASS(response.signature = "s");
	var i = response.iter();
	WVPASSEQ(i.pop(), failmsg);
    }

    [Test, Category("Query Security Tests")]
    public void Testbarrage()
    {
	/* NOTE:  You can't run these tests unless you set a user (say *'s)
	 * security level to 0 (or false).  Since we try to run things with
	 * security level 1 for testing, this test will fail where others
	 * won't.  Commented out.
	 */
    
	/* try
	{
	    WvDbusMsg tosend = setup_msg("select 1");

	    WvDbusMsg response = bus.send_and_wait(tosend);
	    // Warm up
	    confirmpass(response);

	    response = fire_and_retrieve("select 1 select 2");
	    confirmpass(response);

	    response = fire_and_retrieve("create table foo (bar int) select * from foo");
	    confirmpass(response);

	    response = fire_and_retrieve("insert into foo values (1) insert into foo values (2)");
	    confirmfail(response);

	    response = fire_and_retrieve("update foo set bar = 2 select * from foo");
	    confirmpass(response);

	    response = fire_and_retrieve("update foo set bar = 2 set bar = 3");
	    confirmfail(response);

	    response = fire_and_retrieve("alter table foo add c2 int; set dateformat dmy");
	    confirmfail(response);

	    //According to MS docs, this should work.  It doesn't, so we just
	    //make sure we don't get a parse error.
	    response = fire_and_retrieve("alter table foo set (FILESTREAM_ON = \"default\")");
	    WVPASS(response.type = Wv.Dbus.MType.Error);
	    WVPASS(response.signature = "s");
	    var i = response.iter();
	    WVPASSEQ(i.pop(), "Incorrect syntax near the keyword 'set'.");

	    //Here I'm just lazy and didn't feel like making stored procedures
	    //which return the correct parameters
	    response = fire_and_retrieve("insert into foo exec sp_tables");
	    WVPASS(response.type = Wv.Dbus.MType.Error);
	    WVPASS(response.signature = "s");
	    i = response.iter();
	    WVPASSEQ(i.pop(), "Insert Error: Column name or number of supplied values does not match table definition.");

	    confirmfail(fire_and_retrieve("insert into foo values (1) exec sp_tables"));

	    confirmfail(fire_and_retrieve("select 1 insert into foo values (1) select 2"));

	    response = fire_and_retrieve("alter database zoo MODIFY_NAME = poop set dateformat dmy");
	    WVPASS(response.type = Wv.Dbus.MType.Error);
	    WVPASS(response.signature = "s");
	    i = response.iter();
	    WVPASSEQ(i.pop(), "Incorrect syntax near '='.");

	    confirmpass(fire_and_retrieve("begin; select 1; end;"));

	    confirmfail(fire_and_retrieve("begin distributed transaction; update foo set bar = 2"));

	    confirmpass(fire_and_retrieve("update foo set bar = 2; select * from foo"));
	    
	}
	finally
	{
	    VxExec("DROP TABLE foo");
	} */
    }

    public static void Main()
    {
	WvTest.DoMain();
    }
}
