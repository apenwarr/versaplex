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
	log.print(" + VxPutSchema");

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
        //WVPASSEQ(sums.Count, 0);

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
        //WVPASSEQ(sums.Count, 2);

        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVASSERT(sums.ContainsKey("Procedure-Encrypted/EncFunc"));
        WVPASSEQ(sums["Procedure/Func1"].checksums.Count(), 1);
        WVPASSEQ(sums["Procedure-Encrypted/EncFunc"].checksums.Count(), 1);
        WVPASSEQ(sums["Procedure/Func1"].checksums.First(), 0x55F9D9E3);
        WVPASSEQ(sums["Procedure-Encrypted/EncFunc"].checksums.First(), 0xE5E9304F);

        WVASSERT(VxExec("drop procedure EncFunc"));

        sums = dbus.GetChecksums();
        //WVPASSEQ(sums.Count, 1);

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

        // Three columns gives us three checksums
        WVPASSEQ(sums["Table/Tab1"].checksums.Count(), 3);
        WVPASSEQ(sums["Table/Tab1"].checksums.ElementAt(0), 0xE8634548)
        WVPASSEQ(sums["Table/Tab1"].checksums.ElementAt(1), 0xA5F77357)
        WVPASSEQ(sums["Table/Tab1"].checksums.ElementAt(2), 0xE50EE702)

        WVASSERT(VxExec("drop table Tab1"));

        sc.Cleanup();
    }

    [Test, Category("Schemamatic"), Category("GetSchemaChecksums")]
    public void TestIndexChecksums()
    {
        try { VxExec("drop table Tab1"); } catch { }
        string query = "CREATE TABLE [Tab1] (\n" + 
            "\t[f1] [int] NOT NULL PRIMARY KEY,\n" +
            "\t[f2] [money] NULL,\n" + 
            "\t[f3] [varchar] (80) NULL);\n\n";
        WVASSERT(VxExec(query));

        query = "CREATE INDEX [Index1] ON [Tab1] (f1)";
        WVASSERT(VxExec(query));

        query = "CREATE INDEX [Index2] ON [Tab1] (f1, f2)";
        WVASSERT(VxExec(query));

        VxSchemaChecksums sums;
        sums = dbus.GetChecksums();

        WVPASSEQ(sums["Index/Tab1/Index1"].checksums.Count(), 1);
        WVPASSEQ(sums["Index/Tab1/Index1"].checksums.First(), 0x62781FDD);
        // An index on two columns will include two checksums
        WVPASSEQ(sums["Index/Tab1/Index2"].checksums.Count(), 2);
        WVPASSEQ(sums["Index/Tab1/Index2"].checksums.ElementAt(0), 0x603EA184);
        WVPASSEQ(sums["Index/Tab1/Index2"].checksums.ElementAt(1), 0x8FD2C903);

        WVASSERT(VxExec("drop table Tab1"));
    }

    [Test, Category("Schemamatic"), Category("GetSchemaChecksums")]
    public void TestXmlSchemaChecksums()
    {
        SchemaCreator sc = new SchemaCreator(this);
        WVASSERT(VxExec(sc.xmlq));

        VxSchemaChecksums sums;
        sums = dbus.GetChecksums();

        WVPASSEQ(sums["XMLSchema/TestSchema"].checksums.Count(), 1);
        WVPASSEQ(sums["XMLSchema/TestSchema"].checksums.First(), 0xFA7736B3);

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
        VxSchema schema = dbus.Get("Procedure/Func1é");
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
    public void TestGetIndexSchema()
    {
        SchemaCreator sc = new SchemaCreator(this);
        sc.Create();

        // Check that the query limiting works
	VxSchema schema = dbus.Get("Index/Tab1/Idx1");
	WVPASSEQ(schema.Count, 1);

	WVASSERT(schema.ContainsKey("Index/Tab1/Idx1"));
	WVPASSEQ(schema["Index/Tab1/Idx1"].name, "Tab1/Idx1");
	WVPASSEQ(schema["Index/Tab1/Idx1"].type, "Index");
	WVPASSEQ(schema["Index/Tab1/Idx1"].encrypted, false);
	WVPASSEQ(schema["Index/Tab1/Idx1"].text.Length, sc.idx1q.Length);
	WVPASSEQ(schema["Index/Tab1/Idx1"].text, sc.idx1q);

        // Now get everything, since we don't know the primary key's name
        schema = dbus.Get();
        WVASSERT(schema.Count >= 2);

	WVASSERT(schema.ContainsKey("Index/Tab1/Idx1"));
	WVPASSEQ(schema["Index/Tab1/Idx1"].name, "Tab1/Idx1");
	WVPASSEQ(schema["Index/Tab1/Idx1"].type, "Index");
	WVPASSEQ(schema["Index/Tab1/Idx1"].encrypted, false);
	WVPASSEQ(schema["Index/Tab1/Idx1"].text.Length, sc.idx1q.Length);
	WVPASSEQ(schema["Index/Tab1/Idx1"].text, sc.idx1q);

        CheckForPrimaryKey(schema, "Tab1");

        sc.Cleanup();
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
        string query = "CREATE TABLE [Table1] (\n\t" + 
            "[f1] [int]  NOT NULL PRIMARY KEY,\n\t" +
            "[f2] [money]  NULL,\n\t" + 
            "[f3] [varchar] (80) NOT NULL,\n\t" +
            "[f4] [varchar] (max) DEFAULT 'Default Value' NULL,\n\t" + 
            "[f5] [decimal] (3,2),\n\t" + 
            "[f6] [bigint]  NOT NULL IDENTITY(4, 5));\n\n";
        WVASSERT(VxExec(query));

        VxSchema schema = dbus.Get();
        WVASSERT(schema.Count >= 2);

        // Primary keys get returned as indexes, not in the CREATE TABLE
        string result = query.Replace(" PRIMARY KEY", "");
        // Check that columns default to nullable
        result = result.Replace("[f5] [decimal] (3,2)", 
            "[f5] [decimal] (3,2) NULL");

        WVASSERT(schema.ContainsKey("Table/Table1"));
        WVPASSEQ(schema["Table/Table1"].name, "Table1");
        WVPASSEQ(schema["Table/Table1"].type, "Table");
        WVPASSEQ(schema["Table/Table1"].encrypted, false);
        WVPASSEQ(schema["Table/Table1"].text, result);

        CheckForPrimaryKey(schema, "Table1");

        try { VxExec("drop table Table1"); } catch { }
    }

    [Test, Category("Schemamatic"), Category("DropSchema")]
    public void TestDropSchema()
    {
        SchemaCreator sc = new SchemaCreator(this);

        sc.Create();
        
        VxSchemaChecksums sums = dbus.GetChecksums();

        WVASSERT(sums.ContainsKey("Index/Tab1/Idx1"));
        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVASSERT(sums.ContainsKey("ScalarFunction/Func2"));
        WVASSERT(sums.ContainsKey("Table/Tab1"));
        WVASSERT(sums.ContainsKey("Table/Tab2"));
        WVASSERT(sums.ContainsKey("XMLSchema/TestSchema"));

        dbus.DropSchema("Index/Tab1/Idx1", "Procedure/Func1", 
            "ScalarFunction/Func2", "Table/Tab2", "XMLSchema/TestSchema");

        sums = dbus.GetChecksums();

        WVASSERT(!sums.ContainsKey("Index/Tab1/Idx1"));
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
        WVPASSEQ(VxPutSchema("Table", "Tab1", sc.tab1q, no_opts), null);
        WVPASSEQ(VxPutSchema("Index", "Tab1/Idx1", sc.idx1q, no_opts), null);
        WVPASSEQ(VxPutSchema("Procedure", "Func1", sc.func1q, no_opts), null);
        WVPASSEQ(VxPutSchema("XMLSchema", "TestSchema", sc.xmlq, no_opts), null);
        
        VxSchema schema = dbus.Get();

        WVASSERT(schema.ContainsKey("Index/Tab1/Idx1"));
        WVPASSEQ(schema["Index/Tab1/Idx1"].name, "Tab1/Idx1");
        WVPASSEQ(schema["Index/Tab1/Idx1"].type, "Index");
        WVPASSEQ(schema["Index/Tab1/Idx1"].text, sc.idx1q);
        WVASSERT(schema.ContainsKey("Procedure/Func1"));
        WVPASSEQ(schema["Procedure/Func1"].name, "Func1");
        WVPASSEQ(schema["Procedure/Func1"].type, "Procedure");
        WVPASSEQ(schema["Procedure/Func1"].text, sc.func1q);
        WVASSERT(schema.ContainsKey("Table/Tab1"));
        WVPASSEQ(schema["Table/Tab1"].name, "Tab1");
        WVPASSEQ(schema["Table/Tab1"].type, "Table");
        WVPASSEQ(schema["Table/Tab1"].text, sc.tab1q_nopk);
        WVASSERT(schema.ContainsKey("XMLSchema/TestSchema"));
        WVPASSEQ(schema["XMLSchema/TestSchema"].name, "TestSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].type, "XMLSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].text, sc.xmlq);

        string tab1q2 = "CREATE TABLE [Tab1] (\n\t" + 
            "[f4] [binary] (1) NOT NULL);\n\n";

        VxSchemaError err = VxPutSchema("Table", "Tab1", tab1q2, no_opts);
        WVPASS(err != null);
        WVPASSEQ(err.key, "Table/Tab1");
        WVPASSEQ(err.msg, 
            "There is already an object named 'Tab1' in the database.");
        WVPASSEQ(err.errnum, 2714);
        
        schema = dbus.Get("Table/Tab1");
        WVPASSEQ(schema["Table/Tab1"].name, "Tab1");
        WVPASSEQ(schema["Table/Tab1"].type, "Table");
        WVPASSEQ(schema["Table/Tab1"].text, sc.tab1q_nopk);

        WVPASSEQ(VxPutSchema("Table", "Tab1", tab1q2, VxPutOpts.Destructive), 
            null);

        schema = dbus.Get("Table/Tab1");
        WVPASSEQ(schema["Table/Tab1"].name, "Tab1");
        WVPASSEQ(schema["Table/Tab1"].type, "Table");
        WVPASSEQ(schema["Table/Tab1"].text, tab1q2);

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

        WVPASSEQ(VxPutSchema("Table", "Tab1", sc.tab1q, VxPutOpts.None), null);

        List<string> inserts = new List<string>();
        for (int ii = 0; ii < 22; ii++)
        {
            inserts.Add(String.Format("INSERT INTO Tab1 ([f1],[f2],[f3]) " + 
                "VALUES ({0},{1},'{2}');\n", 
                ii, ii + ".3400", "Hi" + ii));
        }

        inserts.Add("INSERT INTO Tab1 ([f1],[f2],[f3]) " +
            "VALUES (100,NULL,'');\n");
        inserts.Add("INSERT INTO Tab1 ([f1],[f2],[f3]) " +
            "VALUES (101,NULL," + 
            "'This string''s good for \"testing\" escaping, isn''t it?');\n");

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

        WVPASSEQ(VxPutSchema("Table", "Tab1", sc.tab1q, VxPutOpts.None), null);

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
        srcsums.AddSum("Index/Tab1/ConflictIndex", 3);
        srcsums.AddSum("Table/HarmonyTable", 6);

        goalsums.AddSum("Table/NewTable", 3);
        goalsums.AddSum("Procedure/NewFunc", 4);
        goalsums.AddSum("Index/Tab1/ConflictIndex", 5);
        goalsums.AddSum("Table/HarmonyTable", 6);

        VxSchemaDiff diff = new VxSchemaDiff(srcsums, goalsums);

        string expected = "- XMLSchema/firstxml\n" + 
            "- XMLSchema/secondxml\n" +
            "+ Table/NewTable\n" +
            "* Index/Tab1/ConflictIndex\n" +
            "+ Procedure/NewFunc\n";
        WVPASSEQ(diff.ToString(), expected);

        // Check that the internal order matches the string's order.
        WVPASSEQ(diff.ElementAt(0).Key, "XMLSchema/firstxml");
        WVPASSEQ(diff.ElementAt(1).Key, "XMLSchema/secondxml");
        WVPASSEQ(diff.ElementAt(2).Key, "Table/NewTable");
        WVPASSEQ(diff.ElementAt(3).Key, "Index/Tab1/ConflictIndex");
        WVPASSEQ(diff.ElementAt(4).Key, "Procedure/NewFunc");

        // Check that a comparison with an empty set of sums returns the other
        // side, sorted.
        diff = new VxSchemaDiff(srcsums, emptysums);
        expected = "- XMLSchema/firstxml\n" + 
            "- XMLSchema/secondxml\n" + 
            "- Table/HarmonyTable\n" + 
            "- Index/Tab1/ConflictIndex\n";
        WVPASSEQ(diff.ToString(), expected);

        diff = new VxSchemaDiff(emptysums, goalsums);
        expected = "+ Table/HarmonyTable\n" +
            "+ Table/NewTable\n" +
            "+ Index/Tab1/ConflictIndex\n" +
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

        newschema["Procedure/Func1"].text = func1q2;
        newsums.AddSum("Procedure/Func1", 123);
        newsums.Remove("XMLSchema/TestSchema");
        origsums.Remove("Index/Tab1/Idx1");

        VxSchemaDiff diff = new VxSchemaDiff(origsums, newsums);
        using (IEnumerator<KeyValuePair<string,VxDiffType>> iter = 
            diff.GetEnumerator())
        {
            WVPASS(iter.MoveNext());
            WVPASSEQ(iter.Current.Key, "XMLSchema/TestSchema");
            WVPASSEQ((char)iter.Current.Value, (char)VxDiffType.Remove);
            WVPASS(iter.MoveNext());
            WVPASSEQ(iter.Current.Key, "Index/Tab1/Idx1");
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
        WVPASSEQ(diffschema["Index/Tab1/Idx1"].type, "Index");
        WVPASSEQ(diffschema["Index/Tab1/Idx1"].name, "Tab1/Idx1");
        WVPASSEQ(diffschema["Index/Tab1/Idx1"].text, sc.idx1q);
        WVPASSEQ(diffschema["Procedure/Func1"].type, "Procedure");
        WVPASSEQ(diffschema["Procedure/Func1"].name, "Func1");
        WVPASSEQ(diffschema["Procedure/Func1"].text, func1q2);

        VxSchemaErrors errs = backend.Put(diffschema, diffsums, VxPutOpts.None);
        WVPASSEQ(errs.Count, 0);

        VxSchema updated = backend.Get(null);
        WVASSERT(!updated.ContainsKey("XMLSchema/TestSchema"));
        WVPASSEQ(updated["Index/Tab1/Idx1"].text, 
            newschema["Index/Tab1/Idx1"].text);
        WVPASSEQ(updated["Procedure/Func1"].text, 
            newschema["Procedure/Func1"].text);
        WVPASSEQ(updated["Table/Tab1"].text, newschema["Table/Tab1"].text);

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

        // FIXME: Test with all the SchemaCreator elements
        WVASSERT(VxExec(sc.tab1q));
        WVASSERT(VxExec(sc.tab2q));

        VxSchema schema = dbus.Get();
        VxPutOpts no_opts = VxPutOpts.None;
        VxSchemaErrors errs = VxPutSchema(schema, no_opts);

        WVPASSEQ(errs.Count, 2);
        WVPASSEQ(errs["Table/Tab1"].key, "Table/Tab1");
        WVPASSEQ(errs["Table/Tab1"].msg, 
            "There is already an object named 'Tab1' in the database.");
        WVPASSEQ(errs["Table/Tab1"].errnum, 2714);
        WVPASSEQ(errs["Table/Tab2"].key, "Table/Tab2");
        WVPASSEQ(errs["Table/Tab2"].msg, 
            "There is already an object named 'Tab2' in the database.");
        WVPASSEQ(errs["Table/Tab2"].errnum, 2714);

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

    public static void Main()
    {
        WvTest.DoMain();
    }
}
