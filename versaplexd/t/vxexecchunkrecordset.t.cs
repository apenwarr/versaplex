#include "wvtest.cs.h"

using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Wv;
using Wv.Test;

[TestFixture]
class VxExecChunkRecordsetTests : VersaplexTester
{
    [Test, Category("ExecChunkRecordset")]
    public void EmptyTableTest()
    {
	WvLog.maxlevel = WvLog.L.Debug5;
	
	// Check that column types are copied correctly to the output table
        try { VxExec("DROP TABLE test1"); } catch {}

	try {
	    WVASSERT(VxExec("CREATE TABLE test1 (numcol int not null)"));

            object result;
            WVASSERT(VxScalar("SELECT COUNT(*) FROM test1", out result));
            WVPASSEQ((int)result, 0);

	    VxColumnInfo[] colinfo1;
	    object[][] data1;
	    bool[][] nullity1;
            WVASSERT(VxChunkRecordset("SELECT * FROM test1", out colinfo1,
					out data1, out nullity1));

	    VxColumnInfo[] colinfo2;
	    object[][] data2;
	    bool[][] nullity2;
            WVASSERT(VxRecordset("SELECT * FROM test1", out colinfo2,
				    out data2, out nullity2));

	    WVASSERT(colinfo1.Length == colinfo2.Length);

	    // Should be good enough, really
	    for (int i = 0; i < colinfo1.Length; ++i)
	    {
		WVASSERT(colinfo1[i].ColumnName == colinfo2[i].ColumnName);
		WVASSERT(colinfo1[i].ColumnType == colinfo2[i].ColumnType);
	    }
        } finally {
            try { VxExec("DROP TABLE test1"); } catch {}
        }
    }


    [Test, Category("ExecChunkRecordset")]
    public void BasicTest()
    {
	// Check that column types are copied correctly to the output table
        try { VxExec("DROP TABLE test1"); } catch {}

	try {
            WVASSERT(VxExec("CREATE TABLE test1 (numcol int, testcol1 TEXT, testcol2 TEXT, testcol3 TEXT, testcol4 TEXT, testcol5 TEXT, testcol6 TEXT, testcol7 TEXT, testcol8 TEXT, testcol9 TEXT, testcol10 TEXT)"));

            object result;
            WVASSERT(VxScalar("SELECT COUNT(*) FROM test1", out result));
            WVPASSEQ((int)result, 0);

	    string basestring = "This is becoming a speech.  You are the captain sir, you are entitled.  I am not entitled to ramble on about something everyone knows.  Captain Jean-Luc Picard of the USS Enterprise {0}.  Captain Jean-Luc Picard of the USS Enterprise {1}. M-M-M-M Make it so.  Make it so.  M-M-M-M Make it so.  Make it so.  He just kept talking in one looooonggg... incredibly unbroken sentence moving from topic to topic very fast so that no one had a chance to interrupt it was really quite hypnotic.  Um.  I am not dressed properly.  There is this theory of the Moebius.  A... rift in the fabric of space, where time becomes a loop.  Where time becomes a loop.  Where time (time) becomes a loop.  The first guiding principle of any Starfleet officer is to the truth.  Be it personal truth, or historical truth.  If you can not find it within yourself to stand up and tell the truth, then you do not deserve to wear that uniform!!!1111  Electric Barbarella is such an awesome tune, it is stuckin my head right now and I CAN NOT GET IT OUT!";
	    for (int i = 0; i < 103; ++i) {
		Exec(string.Format("INSERT INTO test1 (numcol) values ({0})", i));
		string istring = string.Format(basestring, i, i * 2);
		for (int j = 1; j <= 10; ++j)
		{
		    string temp = string.Format("UPDATE test1 SET testcol{0} = '{1}' WHERE numcol = {2}", j, istring, i);
		    //string temp = string.Format("INSERT INTO test1 VALUES (\"{0}\", \"{0}\", \"{0}\", \"{0}\", \"{0}\", \"{0}\", \"{0}\", \"{0}\", \"{0}\", \"{0}\")", istring);
		    WVASSERT(Exec(temp));
		}
	    }

	    VxColumnInfo[] colinfo1;
	    object[][] data1;
	    bool[][] nullity1;
            WVASSERT(VxChunkRecordset("SELECT * FROM test1", out colinfo1,
					out data1, out nullity1));

	    VxColumnInfo[] colinfo2;
	    object[][] data2;
	    bool[][] nullity2;
            WVASSERT(VxRecordset("SELECT * FROM test1", out colinfo2,
				    out data2, out nullity2));


	    WVASSERT(data1.Length == data2.Length);
	    WVASSERT(data1[0].Length == data2[0].Length);

	    Console.WriteLine("About to do outer loop {0} times, " +
				"inner loop {1} times.", data1.Length,
				data1[0].Length);
	    for (int i = 0; i < data1.Length; ++i)
	    {
		for (int j = 0; j < data1[i].Length; ++j)
		{
		    WVASSERT(nullity1[i][j] == nullity2[i][j]);
		    if (!nullity1[i][j])
			WVASSERT(data1[i][j].ToString() ==
				    data2[i][j].ToString());
		}
	    }
        } finally {
            try { VxExec("DROP TABLE test1"); } catch {}
        }
    }


    public static void Main()
    {
	WvTest.DoMain();
    }
}
