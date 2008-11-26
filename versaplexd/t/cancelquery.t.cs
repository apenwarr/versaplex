#include "wvtest.cs.h"

using Wv;
using Wv.Test;

[TestFixture]
class CancelQueryTests : VersaplexTester
{
    private WvDbusMsg setup_msg(string call, string sig, object data)
    {
	WvDbusMsg msg = methodcall(call, sig);

	WvDbusWriter mw = new WvDbusWriter();
	if (sig == "s")
	    mw.Write((string)data);
	if (sig == "u")
	    mw.Write((uint)data);

	msg.Body = mw.ToArray();

	return msg;
    }

    [Test, Category("CancelQuery")]
    public void DisposeTest()
    {
	WvSqlRows foo = dbi.select("select 1");

	//If this doesn't throw an exception, we're golden
	foo.Dispose();
	foo.Dispose();
    }

    [Test, Category("CancelQuery")]
    public void SimpleTest()
    {
	try { VxExec("DROP TABLE test1;"); } catch {}

	try {
	    VxExec("CREATE TABLE test1 (numcol int, stupid VARCHAR(40))");
	    Exec("INSERT INTO test1 VALUES (1, 'Luke is awesome')");
	} catch {}

	//Frivolous crap to fill up the action_queue
	WvDbusMsg call = setup_msg("ExecChunkRecordset", "s", "select 1");
	bus.send(call, (r) => {;});
	bus.send(call, (r) => {;});

	//What we're really worried about here.
	call = setup_msg("ExecChunkRecordset", "s",
	    "UPDATE test1 SET stupid = 'Luke is suck' WHERE numcol = 1");
	uint send_id = bus.send(call, (r) => {;});

	call = setup_msg("CancelQuery", "u", send_id);

	WvDbusMsg rep = bus.send_and_wait(call);

	try {
	    VxColumnInfo[] colinfo1;
	    object[][] data1;
	    bool[][] nullity1;
            WVASSERT(VxChunkRecordset("SELECT * FROM test1", out colinfo1,
					out data1, out nullity1)); 

	    //NOTE:  If this ever fails for you, check the Versaplex logs!
	    //See what's going on with CancelQuery.  If it attempted to cancel
	    //a running query, then if you're using Mono to run Versaplexd, you
	    //might not pass this test, as Mono seems unable to Cancel a
	    //running IDbCommand.  If it passes in Mono, that means the command
	    //being Canceled never made it to execution, and was canceled while
	    //still on the action_queue.
	    WVPASSEQ((string)(data1[0][1]), "Luke is awesome");
	} finally {
	    try { VxExec("DROP TABLE test1"); } catch {}
	}
    }

    public static void Main()
    {
	WvTest.DoMain();
    }
}
