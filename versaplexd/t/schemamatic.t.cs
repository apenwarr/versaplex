#include "wvtest.cs.h"
// Test the Schemamatic functions that live in the Versaplex daemon.

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using System.Security.Cryptography;
using Wv;
using Wv.Extensions;
using Wv.Test;
using NDesk.DBus;

[TestFixture]
class SchemamaticTests : SchemamaticTester
{
    VxDbusSchema dbus;
    WvLog log;

    public SchemamaticTests()
    {
        dbus = new VxDbusSchema(bus);
        log = new WvLog("Schemamatic Tests");
    }

    // Utility function to put a single schema element.
    // This may someday be useful to have in the VxDbusSchema class (or even
    // in ISchemaBackend), but this implementation is a tiny bit too hacky for
    // general consumption.
    VxSchemaError VxPutSchema(string type, string name, string text, 
        VxPutOpts opts)
    {
        VxSchemaElement elem = new VxSchemaElement(type, name, text, false);
        VxSchema schema = new VxSchema(elem);
        VxSchemaErrors errs = VxPutSchema(schema, opts);
        if (errs == null || errs.Count == 0)
            return null;

        WVPASSEQ(errs.Count, 1);

        // Just return the first error
        foreach (KeyValuePair<string,VxSchemaError> p in errs)
            return p.Value;

        WVFAIL("Shouldn't happen: couldn't find error to return");

        return null;
    }

    VxSchemaErrors VxPutSchema(VxSchema schema, VxPutOpts opts)
    {
	log.print(" + VxPutSchema\n");

        return dbus.Put(schema, null, opts);
    }

    [Test, Category("Schemamatic"), Category("GetSchemaChecksums")]
    public void TestProcedureChecksums()
    {
        try { VxExec("drop procedure Func1"); } catch { }
        try { VxExec("drop procedure EncFunc"); } catch { }

        VxSchemaChecksums sums;
        sums = dbus.GetChecksums();
        if (sums.Count != 0)
        {
            log.print("Found entries:");
            foreach (KeyValuePair<string,VxSchemaChecksum> p in sums)
                log.print(p.Key);
        }
        // Sometimes the database has stray elements; just check that our 
        // changes add and remove the expected number.  It'd be nice to 
        // have a proper test that an empty database returns an empty 
        // set of strings, but this will have to do.
        int baseline_sum_count = sums.Count;

        string msg1 = "Hello, world, this is Func1!";
        string msg2 = "Hello, world, this is Func2!";
        object outmsg;
        WVASSERT(VxExec("create procedure Func1 as select '" + msg1 + "'"));
        WVASSERT(VxScalar("exec Func1", out outmsg));
        WVPASSEQ(msg1, (string)outmsg);

        sums = dbus.GetChecksums();

        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVPASSEQ(sums["Procedure/Func1"].checksums.Count(), 1);
        WVPASSEQ(sums["Procedure/Func1"].checksums.First(), 0x55F9D9E3);

        WVASSERT(VxExec("create procedure EncFunc with encryption as select '" + 
            msg2 + "'"));

        WVASSERT(VxScalar("exec EncFunc", out outmsg));
        WVPASSEQ(msg2, (string)outmsg);

        sums = dbus.GetChecksums();
        WVPASSEQ(sums.Count, baseline_sum_count + 2);

        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVASSERT(sums.ContainsKey("Procedure-Encrypted/EncFunc"));
        WVPASSEQ(sums["Procedure/Func1"].checksums.Count(), 1);
        WVPASSEQ(sums["Procedure-Encrypted/EncFunc"].checksums.Count(), 1);
        WVPASSEQ(sums["Procedure/Func1"].checksums.First(), 0x55F9D9E3);
        WVPASSEQ(sums["Procedure-Encrypted/EncFunc"].checksums.First(), 0xE5E9304F);

        WVASSERT(VxExec("drop procedure EncFunc"));

        sums = dbus.GetChecksums();
        WVPASSEQ(sums.Count, baseline_sum_count + 1);

        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVFAIL(sums.ContainsKey("Procedure/EncFunc"));
        WVPASSEQ(sums["Procedure/Func1"].checksums.Count(), 1);
        WVPASSEQ(sums["Procedure/Func1"].checksums.First(), 0x55F9D9E3);

        WVASSERT(VxExec("drop procedure Func1"));
    }

    [Test, Category("Schemamatic"), Category("GetSchemaChecksums")]
    public void TestTableChecksums()
    {
        SchemaCreator sc = new SchemaCreator(this);
        sc.Create();

        VxSchemaChecksums sums;
        sums = dbus.GetChecksums();

        // Three columns, and two indexes each with two columns, gives us 
        // seven checksums
        WVPASSEQ(sums["Table/Tab1"].checksums.Count(), 7);
        WVPASSEQ(sums["Table/Tab1"].checksums.ElementAt(0), 0x00B0B636)
        WVPASSEQ(sums["Table/Tab1"].checksums.ElementAt(1), 0x1D32C7EA)
        WVPASSEQ(sums["Table/Tab1"].checksums.ElementAt(2), 0x968DBEDC)
        WVPASSEQ(sums["Table/Tab1"].checksums.ElementAt(3), 0xAB109B86)
        WVPASSEQ(sums["Table/Tab1"].checksums.ElementAt(4), 0xC1A74EA4)
        WVPASSEQ(sums["Table/Tab1"].checksums.ElementAt(5), 0xE50EE702)
        WVPASSEQ(sums["Table/Tab1"].checksums.ElementAt(6), 0xE8634548)

        WVASSERT(VxExec("drop table Tab1"));

        sc.Cleanup();
    }

    [Test, Category("Schemamatic"), Category("GetSchemaChecksums")]
    public void TestXmlSchemaChecksums()
    {
        SchemaCreator sc = new SchemaCreator(this);
        WVASSERT(VxExec(sc.xmlq));

        VxSchemaChecksums sums;
        sums = dbus.GetChecksums();

        WVPASSEQ(sums["XMLSchema/TestSchema"].checksums.Count(), 1);
        WVPASSEQ(sums["XMLSchema/TestSchema"].checksums.First(), 4105357156);

        WVASSERT(VxExec("drop xml schema collection TestSchema"));

        sc.Cleanup();
    }

    [Test, Category("Schemamatic"), Category("GetSchema")]
    public void TestGetProcSchema()
    {
        try { VxExec("drop procedure [Func1!]"); } catch { }
        try { VxExec("drop procedure [Func2]"); } catch { }

        string msg1 = "Hello, world, this is Func1!";
        string msg2 = "Hello, world, this is Func2!";
        string fmt = "create procedure [{0}] {1} as select '{2}'";
        string query1 = String.Format(fmt, "Func1!", "", msg1);
        string query2 = String.Format(fmt, "Func2", "with encryption", msg2);
        object outmsg;
        WVASSERT(VxExec(query1));
        WVASSERT(VxScalar("exec [Func1!]", out outmsg));
        WVPASSEQ(msg1, (string)outmsg);

        WVASSERT(VxExec(query2));
        WVASSERT(VxScalar("exec [Func2]", out outmsg));
        WVPASSEQ(msg2, (string)outmsg);

        // Check that the query limiting works.  Also test that the evil
        // character cleansing works (turning bad characters into !s)
        VxSchema schema = dbus.Get("Procedure/NonExistentFunction");
        WVPASSEQ(schema.Count, 0);

        schema = dbus.Get("Procedure/Func1é");
        WVPASSEQ(schema.Count, 1);

        WVASSERT(schema.ContainsKey("Procedure/Func1!"));
        WVPASSEQ(schema["Procedure/Func1!"].name, "Func1!");
        WVPASSEQ(schema["Procedure/Func1!"].type, "Procedure");
        WVPASSEQ(schema["Procedure/Func1!"].encrypted, false);
        WVPASSEQ(schema["Procedure/Func1!"].text, query1);

        schema = dbus.Get("Func1é");
        WVPASSEQ(schema.Count, 1);

        WVASSERT(schema.ContainsKey("Procedure/Func1!"));
        WVPASSEQ(schema["Procedure/Func1!"].name, "Func1!");
        WVPASSEQ(schema["Procedure/Func1!"].type, "Procedure");
        WVPASSEQ(schema["Procedure/Func1!"].encrypted, false);
        WVPASSEQ(schema["Procedure/Func1!"].text, query1);

        // Also check that unlimited queries get everything
        schema = dbus.Get();
        WVASSERT(schema.Count >= 2);

        WVASSERT(schema.ContainsKey("Procedure/Func1!"));
        WVPASSEQ(schema["Procedure/Func1!"].name, "Func1!");
        WVPASSEQ(schema["Procedure/Func1!"].type, "Procedure");
        WVPASSEQ(schema["Procedure/Func1!"].encrypted, false);
        WVPASSEQ(schema["Procedure/Func1!"].text, query1);

        WVASSERT(schema.ContainsKey("Procedure-Encrypted/Func2"));
        WVPASSEQ(schema["Procedure-Encrypted/Func2"].name, "Func2");
        WVPASSEQ(schema["Procedure-Encrypted/Func2"].type, "Procedure");
        WVPASSEQ(schema["Procedure-Encrypted/Func2"].encrypted, true);
        // FIXME: Can't yet retrieve the contents of encrypted functions
        //WVPASSEQ(schema["Procedure-Encrypted/Func2"].text, query2);

        WVASSERT(VxExec("drop procedure [Func1!]"));
        WVASSERT(VxExec("drop procedure [Func2]"));
    }

    [Test, Category("Schemamatic"), Category("GetSchema")]
    public void TestGetXmlSchemas()
    {
        SchemaCreator sc = new SchemaCreator(this);

        try { VxExec("drop xml schema collection TestSchema"); } catch { }
        try { VxExec("drop xml schema collection TestSchema2"); } catch { }

        string query1 = sc.xmlq;
        WVASSERT(VxExec(query1));

	// Make a long XML Schema, to test the 4000-character chunking
        string query2 = "\nCREATE XML SCHEMA COLLECTION [dbo].[TestSchema2] AS " + 
            "'<xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">" + 
            "<xsd:element name=\"Employee\">" +
             "<xsd:complexType>" +
              "<xsd:complexContent>" + 
               "<xsd:restriction base=\"xsd:anyType\">" + 
                "<xsd:sequence>";
	
        while (query2.Length < 8000)
            query2 += 
                "<xsd:element name=\"SIN\" type=\"xsd:string\"/>" +
                "<xsd:element name=\"Name\" type=\"xsd:string\"/>" +
                "<xsd:element name=\"DateOfBirth\" type=\"xsd:date\"/>" +
                "<xsd:element name=\"EmployeeType\" type=\"xsd:string\"/>" +
                "<xsd:element name=\"Salary\" type=\"xsd:long\"/>";

        query2 += "</xsd:sequence>" + 
                "</xsd:restriction>" + 
               "</xsd:complexContent>" + 
              "</xsd:complexType>" +
             "</xsd:element>" +
            "</xsd:schema>'\n";

        WVASSERT(VxExec(query2));

        // Test that the query limiting works
	VxSchema schema = dbus.Get("TestSchema");
        WVPASSEQ(schema.Count, 1);

        WVASSERT(schema.ContainsKey("XMLSchema/TestSchema"));
        WVPASSEQ(schema["XMLSchema/TestSchema"].name, "TestSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].type, "XMLSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].encrypted, false);
        WVPASSEQ(schema["XMLSchema/TestSchema"].text, query1);

	schema = dbus.Get("XMLSchema/TestSchema");
        WVPASSEQ(schema.Count, 1);

        WVASSERT(schema.ContainsKey("XMLSchema/TestSchema"));
        WVPASSEQ(schema["XMLSchema/TestSchema"].name, "TestSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].type, "XMLSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].encrypted, false);
        WVPASSEQ(schema["XMLSchema/TestSchema"].text, query1);

        // Also check that unlimited queries get everything
	schema = dbus.Get();
        WVASSERT(schema.Count >= 2)

        WVASSERT(schema.ContainsKey("XMLSchema/TestSchema"));
        WVPASSEQ(schema["XMLSchema/TestSchema"].name, "TestSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].type, "XMLSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].encrypted, false);
        WVPASSEQ(schema["XMLSchema/TestSchema"].text, query1);

        WVASSERT(schema.ContainsKey("XMLSchema/TestSchema2"));
        WVPASSEQ(schema["XMLSchema/TestSchema2"].name, "TestSchema2");
        WVPASSEQ(schema["XMLSchema/TestSchema2"].type, "XMLSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema2"].encrypted, false);
        WVPASSEQ(schema["XMLSchema/TestSchema2"].text, query2);

        WVASSERT(VxExec("drop xml schema collection TestSchema"));
        WVASSERT(VxExec("drop xml schema collection TestSchema2"));
    }

    [Test, Category("Schemamatic"), Category("GetSchema")]
    public void TestGetTableSchema()
    {
        try { VxExec("drop table Table1"); } catch { }
        // Name the primary key PK_Table1 to test that GetSchema properly
        // omits the default name.
        // FIXME: Should also test that it does give us a non-default name.
        string query = "CREATE TABLE [Table1] (\n\t" + 
            "[f1] [int]  NOT NULL,\n\t" +
            "[f2] [money]  NULL,\n\t" + 
            "[f3] [varchar] (80) NOT NULL,\n\t" +
            "[f4] [varchar] (max) DEFAULT 'Default Value' NULL,\n\t" + 
            "[f5] [decimal] (3,2),\n\t" + 
            "[f6] [bigint]  NOT NULL IDENTITY(4, 5));\n\n" + 
            "ALTER TABLE [Table1] ADD CONSTRAINT [PK_Table1] PRIMARY KEY (f1)\n";
        WVASSERT(VxExec(query));

        VxSchema schema = dbus.Get();
        WVASSERT(schema.Count >= 1);

        string tab1schema = "column: name=f1,type=int,null=0\n" + 
            "column: name=f2,type=money,null=1\n" + 
            "column: name=f3,type=varchar,null=0,length=80\n" + 
            "column: name=f4,type=varchar,null=1,length=max,default='Default Value'\n" + 
            "column: name=f5,type=decimal,null=1,precision=3,scale=2\n" + 
            "column: name=f6,type=bigint,null=0,identity_seed=4,identity_incr=5\n" + 
            "primary-key: column=f1,clustered=1\n";

        log.print("Retrieved: " + schema["Table/Table1"].text);
        log.print("Expected: " + tab1schema);

        WVASSERT(schema.ContainsKey("Table/Table1"));
        WVPASSEQ(schema["Table/Table1"].name, "Table1");
        WVPASSEQ(schema["Table/Table1"].type, "Table");
        WVPASSEQ(schema["Table/Table1"].encrypted, false);
        WVPASSEQ(schema["Table/Table1"].text, tab1schema);

        try { VxExec("drop table Table1"); } catch { }
    }

    [Test, Category("Schemamatic"), Category("DropSchema")]
    public void TestDropSchema()
    {
        SchemaCreator sc = new SchemaCreator(this);

        sc.Create();
        
        VxSchemaChecksums sums = dbus.GetChecksums();

        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVASSERT(sums.ContainsKey("ScalarFunction/Func2"));
        WVASSERT(sums.ContainsKey("Table/Tab1"));
        WVASSERT(sums.ContainsKey("Table/Tab2"));
        WVASSERT(sums.ContainsKey("XMLSchema/TestSchema"));

        dbus.DropSchema("Procedure/Func1", "ScalarFunction/Func2", 
            "Table/Tab2", "XMLSchema/TestSchema");

        sums = dbus.GetChecksums();

        WVASSERT(!sums.ContainsKey("Procedure/Func1"));
        WVASSERT(!sums.ContainsKey("ScalarFunction/Func2"));
        WVASSERT(sums.ContainsKey("Table/Tab1"));
        WVASSERT(!sums.ContainsKey("Table/Tab2"));
        WVASSERT(!sums.ContainsKey("XMLSchema/TestSchema"));

        VxSchemaErrors errs = dbus.DropSchema("Procedure/Func1");
        WVPASSEQ(errs.Count, 1);
        WVPASSEQ(errs["Procedure/Func1"].msg, 
            "Cannot drop the procedure 'Func1', because it does not exist " + 
            "or you do not have permission.");

        sc.Cleanup();
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestPutSchema()
    {
        SchemaCreator sc = new SchemaCreator(this);

        VxPutOpts no_opts = VxPutOpts.None;
        WVPASSEQ(VxPutSchema("Table", "Tab1", sc.tab1sch, no_opts), null);
        WVPASSEQ(VxPutSchema("Procedure", "Func1", sc.func1q, no_opts), null);
        WVPASSEQ(VxPutSchema("XMLSchema", "TestSchema", sc.xmlq, no_opts), null);
        
        VxSchema schema = dbus.Get();

        WVASSERT(schema.ContainsKey("Procedure/Func1"));
        WVPASSEQ(schema["Procedure/Func1"].name, "Func1");
        WVPASSEQ(schema["Procedure/Func1"].type, "Procedure");
        WVPASSEQ(schema["Procedure/Func1"].text, sc.func1q);
        WVASSERT(schema.ContainsKey("Table/Tab1"));
        WVPASSEQ(schema["Table/Tab1"].name, "Tab1");
        WVPASSEQ(schema["Table/Tab1"].type, "Table");
        WVPASSEQ(schema["Table/Tab1"].text, sc.tab1sch);
        WVASSERT(schema.ContainsKey("XMLSchema/TestSchema"));
        WVPASSEQ(schema["XMLSchema/TestSchema"].name, "TestSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].type, "XMLSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].text, sc.xmlq);

        string tab1sch2 = "column: name=f4,type=binary,null=0,length=1\n";

        VxSchemaError err = VxPutSchema("Table", "Tab1", tab1sch2, no_opts);
        WVPASS(err != null);
        WVPASSEQ(err.key, "Table/Tab1");
        WVPASSEQ(err.msg, 
            "Refusing to drop columns ([f1], [f2], [f3]) when the destructive option is not set.");
        WVPASSEQ(err.errnum, -1);
        
        schema = dbus.Get("Table/Tab1");
        WVPASSEQ(schema["Table/Tab1"].name, "Tab1");
        WVPASSEQ(schema["Table/Tab1"].type, "Table");
        WVPASSEQ(schema["Table/Tab1"].text, sc.tab1sch);

        WVPASSEQ(VxPutSchema("Table", "Tab1", tab1sch2, VxPutOpts.Destructive), 
            null);

        schema = dbus.Get("Table/Tab1");
        WVPASSEQ(schema["Table/Tab1"].name, "Tab1");
        WVPASSEQ(schema["Table/Tab1"].type, "Table");
        WVPASSEQ(schema["Table/Tab1"].text, tab1sch2);

        string msg2 = "This is definitely not the Func1 you thought you knew.";
        string func1q2 = "create procedure Func1 as select '" + msg2 + "'";
        WVPASSEQ(VxPutSchema("Procedure", "Func1", func1q2, no_opts), null);

        schema = dbus.Get("Procedure/Func1");
        WVPASSEQ(schema["Procedure/Func1"].name, "Func1");
        WVPASSEQ(schema["Procedure/Func1"].type, "Procedure");
        WVPASSEQ(schema["Procedure/Func1"].text, func1q2);

        sc.Cleanup();
    }

    [Test, Category("Schemamatic"), Category("SchemaData")]
    public void TestSchemaData()
    {
        SchemaCreator sc = new SchemaCreator(this);

        WVPASSEQ(VxPutSchema("Table", "Tab1", sc.tab1sch, VxPutOpts.None), null);

        List<string> inserts = new List<string>();
        for (int ii = 0; ii < 22; ii++)
        {
            inserts.Add(String.Format("INSERT INTO Tab1 ([f1],[f2],[f3]) " + 
                "VALUES ({0},{1},'{2}');\n", 
                ii, ii + ".3400", "Hi" + ii));
        }

        inserts.Add("INSERT INTO Tab1 ([f1],[f2],[f3]) " +
            "VALUES (101,123.4567," + 
            "'This string''s good for \"testing\" escaping, isn''t it?');\n");
        inserts.Add("INSERT INTO Tab1 ([f1],[f2],[f3]) " +
            "VALUES (100,234.5678,NULL);\n");

        foreach (string ins in inserts)
            WVASSERT(VxExec(ins));

        WVPASSEQ(dbus.GetSchemaData("Tab1", 0, ""), inserts.Join(""));

        VxExec("drop table Tab1");

        try {
            WVEXCEPT(dbus.GetSchemaData("Tab1", 0, ""));
	} catch (Wv.Test.WvAssertionFailure e) {
	    throw e;
	} catch (System.Exception e) {
	    WVPASS(e is DbusError);
            WVPASSEQ(e.Message, "vx.db.sqlerror: Invalid object name 'Tab1'.");
            log.print(e.ToString() + "\n");
	}

        WVPASSEQ(VxPutSchema("Table", "Tab1", sc.tab1sch, VxPutOpts.None), null);

        WVPASSEQ(dbus.GetSchemaData("Tab1", 0, ""), "");

        dbus.PutSchemaData("Tab1", inserts.Join(""), 0);
        WVPASSEQ(dbus.GetSchemaData("Tab1", 0, ""), inserts.Join(""));

        WVPASSEQ(dbus.GetSchemaData("Tab1", 0, "f1 = 11"), 
            "INSERT INTO Tab1 ([f1],[f2],[f3]) VALUES (11,11.3400,'Hi11');\n");

        sc.Cleanup();
    }

    [Test, Category("Schemamatic"), Category("VxSchemaDiff")]
    public void TestChecksumDiff()
    {
        VxSchemaChecksums srcsums = new VxSchemaChecksums();
        VxSchemaChecksums goalsums = new VxSchemaChecksums();
        VxSchemaChecksums emptysums = new VxSchemaChecksums();

        srcsums.AddSum("XMLSchema/secondxml", 2);
        srcsums.AddSum("XMLSchema/firstxml", 1);
        srcsums.AddSum("Procedure/ConflictProc", 3);
        srcsums.AddSum("Table/HarmonyTable", 6);

        goalsums.AddSum("Table/NewTable", 3);
        goalsums.AddSum("Procedure/NewFunc", 4);
        goalsums.AddSum("Procedure/ConflictProc", 5);
        goalsums.AddSum("Table/HarmonyTable", 6);

        VxSchemaDiff diff = new VxSchemaDiff(srcsums, goalsums);

        string expected = "- XMLSchema/firstxml\n" + 
            "- XMLSchema/secondxml\n" +
            "+ Table/NewTable\n" +
            "* Procedure/ConflictProc\n" +
            "+ Procedure/NewFunc\n";
        WVPASSEQ(diff.ToString(), expected);

        // Check that the internal order matches the string's order.
        WVPASSEQ(diff.ElementAt(0).Key, "XMLSchema/firstxml");
        WVPASSEQ(diff.ElementAt(1).Key, "XMLSchema/secondxml");
        WVPASSEQ(diff.ElementAt(2).Key, "Table/NewTable");
        WVPASSEQ(diff.ElementAt(3).Key, "Procedure/ConflictProc");
        WVPASSEQ(diff.ElementAt(4).Key, "Procedure/NewFunc");

        // Check that a comparison with an empty set of sums returns the other
        // side, sorted.
        diff = new VxSchemaDiff(srcsums, emptysums);
        expected = "- XMLSchema/firstxml\n" + 
            "- XMLSchema/secondxml\n" + 
            "- Table/HarmonyTable\n" + 
            "- Procedure/ConflictProc\n";
        WVPASSEQ(diff.ToString(), expected);

        diff = new VxSchemaDiff(emptysums, goalsums);
        expected = "+ Table/HarmonyTable\n" +
            "+ Table/NewTable\n" +
            "+ Procedure/ConflictProc\n" +
            "+ Procedure/NewFunc\n";
        WVPASSEQ(diff.ToString(), expected);
    }

    public void TestApplySchemaDiff(ISchemaBackend backend)
    {
        log.print("In TestApplySchemaDiff({0})\n", backend.GetType().ToString());
        SchemaCreator sc = new SchemaCreator(this);
        sc.Create();

        string msg2 = "Hello, world, this used to be Func1!";
        string func1q2 = "create procedure Func1 as select '" + msg2 + "'\n";
        
        VxSchema origschema = dbus.Get();
        VxSchemaChecksums origsums = dbus.GetChecksums();
        VxSchema newschema = new VxSchema(origschema);
        VxSchemaChecksums newsums = new VxSchemaChecksums(origsums);

        // Don't bother putting the data again if we're talking to dbus: 
        // we already snuck it in the back door.
        if (backend != dbus)
            backend.Put(origschema, origsums, VxPutOpts.None);

        VxSchemaChecksums diffsums = new VxSchemaChecksums(newsums);

        // Make some changes to create an interesting diff.
        // Change the text and sums of Func1, schedule TestSchema for
        // deletion, and act like Tab2 is new.
        newschema["Procedure/Func1"].text = func1q2;
        newsums.AddSum("Procedure/Func1", 123);
        newsums.Remove("XMLSchema/TestSchema");
        origsums.Remove("Table/Tab2");
        WVASSERT(VxExec("drop table Tab2"));

        VxSchemaDiff diff = new VxSchemaDiff(origsums, newsums);
        using (IEnumerator<KeyValuePair<string,VxDiffType>> iter = 
            diff.GetEnumerator())
        {
            WVPASS(iter.MoveNext());
            WVPASSEQ(iter.Current.Key, "XMLSchema/TestSchema");
            WVPASSEQ((char)iter.Current.Value, (char)VxDiffType.Remove);
            WVPASS(iter.MoveNext());
            WVPASSEQ(iter.Current.Key, "Table/Tab2");
            WVPASSEQ((char)iter.Current.Value, (char)VxDiffType.Add);
            WVPASS(iter.MoveNext());
            WVPASSEQ(iter.Current.Key, "Procedure/Func1");
            WVPASSEQ((char)iter.Current.Value, (char)VxDiffType.Change);
            WVFAIL(iter.MoveNext());
        }

        VxSchema diffschema = newschema.GetDiffElements(diff);
        WVPASSEQ(diffschema["XMLSchema/TestSchema"].type, "XMLSchema");
        WVPASSEQ(diffschema["XMLSchema/TestSchema"].name, "TestSchema");
        WVPASSEQ(diffschema["XMLSchema/TestSchema"].text, "");
        WVPASSEQ(diffschema["Table/Tab2"].type, "Table");
        WVPASSEQ(diffschema["Table/Tab2"].name, "Tab2");
        WVPASSEQ(diffschema["Table/Tab2"].text, sc.tab2sch);
        WVPASSEQ(diffschema["Procedure/Func1"].type, "Procedure");
        WVPASSEQ(diffschema["Procedure/Func1"].name, "Func1");
        WVPASSEQ(diffschema["Procedure/Func1"].text, func1q2);

        VxSchemaErrors errs = backend.Put(diffschema, diffsums, VxPutOpts.None);
        WVPASSEQ(errs.Count, 0);

        VxSchema updated = backend.Get(null);
        WVASSERT(!updated.ContainsKey("XMLSchema/TestSchema"));
        WVPASSEQ(updated["Table/Tab1"].text, newschema["Table/Tab1"].text);
        WVPASSEQ(updated["Table/Tab2"].text, newschema["Table/Tab2"].text);
        WVPASSEQ(updated["Procedure/Func1"].text, 
            newschema["Procedure/Func1"].text);

        sc.Cleanup();
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestApplySchemaDiff()
    {
        log.print("Testing applying diffs through DBus");
        TestApplySchemaDiff(dbus);

        log.print("Testing applying diffs to the disk");

        string tmpdir = GetTempDir();
        try
        {
            Directory.CreateDirectory(tmpdir);
            VxDiskSchema disk = new VxDiskSchema(tmpdir);

            TestApplySchemaDiff(disk);
        }
        finally
        {
            Directory.Delete(tmpdir, true);
            WVPASS(!Directory.Exists(tmpdir));
        }
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestPutSchemaErrors()
    {
        SchemaCreator sc = new SchemaCreator(this);

        // Establish a baseline of errors.
        VxPutOpts no_opts = VxPutOpts.None;
        VxSchema schema = dbus.Get();
        VxSchemaErrors errs = VxPutSchema(schema, no_opts);

        int baseline_err_count = errs.Count;

        sc.Create();

        // Try again with the new elements, this time for keeps.

        // Check that putting the same elements doesn't cause errors
        schema = dbus.Get();
        errs = VxPutSchema(schema, no_opts);

        WVPASSEQ(errs.Count, baseline_err_count);

        // Check that invalid SQL causes errors.

        schema = new VxSchema();
        schema.Add("ScalarFunction", "ErrSF", "I am not valid SQL", false);
        schema.Add("TableFunction", "ErrTF", "I'm not valid SQL either", false);

        errs = VxPutSchema(schema, no_opts);

        foreach (var err in errs)
            log.print("Error='{0}'\n", err.Value.ToString());

        WVPASSEQ(errs.Count, baseline_err_count + 2);

	log.print("Results: [\n{0}]\n", errs.Join("'\n'"));
        WVPASSEQ(errs["ScalarFunction/ErrSF"].key, "ScalarFunction/ErrSF");
        WVPASSEQ(errs["ScalarFunction/ErrSF"].msg, 
            "Incorrect syntax near the keyword 'not'.");
        WVPASSEQ(errs["ScalarFunction/ErrSF"].errnum, 156);
        WVPASSEQ(errs["TableFunction/ErrTF"].key, "TableFunction/ErrTF");
        WVPASSEQ(errs["TableFunction/ErrTF"].msg, 
            "Unclosed quotation mark after the character string 'm not valid SQL either'.");
        WVPASSEQ(errs["TableFunction/ErrTF"].errnum, 105);

        sc.Cleanup();
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestPutSchemaRetry()
    {
        try { VxExec("drop view View1"); } catch { }
        try { VxExec("drop view View2"); } catch { }
        try { VxExec("drop view View3"); } catch { }
        try { VxExec("drop view View4"); } catch { }

        // Create the views in the wrong order, so it'll take a few tries
        // to get them all working.  The server seems to sort them
        // alphabetically when it runs them, though this isn't a guarantee.
	string view1q = "create view View1 as select * from View2";
	string view2q = "create view View2 as select * from View3";
	string view3q = "create view View3 as select * from View4";
        string view4q = "create view View4(viewcol1) as select 42";

        VxSchema schema = new VxSchema();
        schema.Add("View", "View1", view1q, false);
        schema.Add("View", "View2", view2q, false);
        schema.Add("View", "View3", view3q, false);
        schema.Add("View", "View4", view4q, false);

        VxSchemaErrors errs = VxPutSchema(schema, VxPutOpts.NoRetry);

        WVPASSEQ(errs.Count, 3);
        WVPASSEQ(errs["View/View1"].key, "View/View1");
        WVPASSEQ(errs["View/View2"].key, "View/View2");
        WVPASSEQ(errs["View/View3"].key, "View/View3");
        WVPASSEQ(errs["View/View1"].msg, "Invalid object name 'View2'.");
        WVPASSEQ(errs["View/View2"].msg, "Invalid object name 'View3'.");
        WVPASSEQ(errs["View/View3"].msg, "Invalid object name 'View4'.");
        WVPASSEQ(errs["View/View1"].errnum, 208);
        WVPASSEQ(errs["View/View2"].errnum, 208);
        WVPASSEQ(errs["View/View3"].errnum, 208);

        try { VxExec("drop view View4"); } catch { }
        errs = VxPutSchema(schema, VxPutOpts.None);
        WVPASSEQ(errs.Count, 0);

        object result;
        WVASSERT(VxScalar("select viewcol1 from View1;", out result));
        WVPASSEQ((int)result, 42);

        try { VxExec("drop view View1"); } catch { }
        try { VxExec("drop view View2"); } catch { }
        try { VxExec("drop view View3"); } catch { }
        try { VxExec("drop view View4"); } catch { }
    }

    // Make sure we can insert double-quoted strings, i.e. that 
    // the database has done a "set quoted_identifiers off"
    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestQuotedIdentifiers()
    {
        try { VxExec("drop procedure Proc1"); } catch { }

        string quote_proc = "create procedure Proc1 as " + 
            "select \"I'm a double-quoted string!\"\n";
        VxSchema schema = new VxSchema();
        schema.Add("Procedure", "Proc1", quote_proc, false);

        VxSchemaErrors errs = VxPutSchema(schema, VxPutOpts.None);

        WVPASSEQ(errs.Count, 0);

        try { VxExec("drop procedure Proc1"); } catch { }
    }

    [Test, Category("Schemamatic"), Category("VxDbSchema")]
    public void TestStripMatchingParens()
    {
        // Make sure that multiple levels of parens get stripped
        WVPASSEQ(VxDbSchema.StripMatchingParens("foo"), "foo");
        WVPASSEQ(VxDbSchema.StripMatchingParens("(foo)"), "foo");
        WVPASSEQ(VxDbSchema.StripMatchingParens("((foo))"), "foo");
        WVPASSEQ(VxDbSchema.StripMatchingParens("(((foo)))"), "foo");
        // Check that we don't strip too many
        WVPASSEQ(VxDbSchema.StripMatchingParens("((2)-(1))"), "(2)-(1)");
        WVPASSEQ(VxDbSchema.StripMatchingParens("((1900)-(01))-(01)"), 
            "((1900)-(01))-(01)");
        WVPASSEQ(VxDbSchema.StripMatchingParens("(((1900)-(01))-(01))"), 
            "((1900)-(01))-(01)");
        // Check what happens with mismatched parens.  
        WVPASSEQ(VxDbSchema.StripMatchingParens("((foo)"), "(foo");
        WVPASSEQ(VxDbSchema.StripMatchingParens("(fo)o)"), "(fo)o)");
        WVPASSEQ(VxDbSchema.StripMatchingParens("(fo)o"), "(fo)o");
        WVPASSEQ(VxDbSchema.StripMatchingParens("(f(oo))"), "f(oo)");
        WVPASSEQ(VxDbSchema.StripMatchingParens("((f(oo))"), "f(oo");
        WVPASSEQ(VxDbSchema.StripMatchingParens("((f)oo))"), "(f)oo)");
        // Check that single-quote escaping works.
        WVPASSEQ(VxDbSchema.StripMatchingParens("('(foo)')"), "'(foo)'");
        WVPASSEQ(VxDbSchema.StripMatchingParens("('foo)')"), "'foo)'");
        WVPASSEQ(VxDbSchema.StripMatchingParens("('foo'')')"), "'foo'')'");
        // Double-quote escaping doesn't work though.
        WVPASSEQ(VxDbSchema.StripMatchingParens("(\"(foo)\")"), "\"(foo)\"");
        WVPASSEQ(VxDbSchema.StripMatchingParens("(\"foo)\")"), "(\"foo)\")");
        WVPASSEQ(VxDbSchema.StripMatchingParens("(\"foo'')\")"), "(\"foo'')\")");
    }

    public static void Main()
    {
        WvTest.DoMain();
    }
}
