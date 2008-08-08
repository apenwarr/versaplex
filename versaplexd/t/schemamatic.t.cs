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
class SchemamaticTests : VersaplexTester
{
    VxDbusSchema dbus;

    public SchemamaticTests()
    {
        dbus = new VxDbusSchema(bus);
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
	Console.WriteLine(" + VxPutSchema");

        return dbus.Put(schema, null, opts);
    }

    [Test, Category("Schemamatic"), Category("GetSchemaChecksums")]
    public void TestProcedureChecksums()
    {
        try { VxExec("drop procedure Func1"); } catch { }
        try { VxExec("drop procedure Func2"); } catch { }

        VxSchemaChecksums sums;
        sums = dbus.GetChecksums();
        if (sums.Count != 0)
        {
            Console.WriteLine("Found entries:");
            foreach (KeyValuePair<string,VxSchemaChecksum> p in sums)
                Console.WriteLine(p.Key);
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
        WVPASSEQ(sums["Procedure/Func1"].checksums.Count, 1);
        WVPASSEQ(sums["Procedure/Func1"].checksums[0], 0x55F9D9E3);

        WVASSERT(VxExec("create procedure Func2 with encryption as select '" + 
            msg2 + "'"));

        WVASSERT(VxScalar("exec Func2", out outmsg));
        WVPASSEQ(msg2, (string)outmsg);

        sums = dbus.GetChecksums();
        //WVPASSEQ(sums.Count, 2);

        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVASSERT(sums.ContainsKey("Procedure-Encrypted/Func2"));
        WVPASSEQ(sums["Procedure/Func1"].checksums.Count, 1);
        WVPASSEQ(sums["Procedure-Encrypted/Func2"].checksums.Count, 1);
        WVPASSEQ(sums["Procedure/Func1"].checksums[0], 0x55F9D9E3);
        WVPASSEQ(sums["Procedure-Encrypted/Func2"].checksums[0], 0x458D4283);

        WVASSERT(VxExec("drop procedure Func2"));

        sums = dbus.GetChecksums();
        //WVPASSEQ(sums.Count, 1);

        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVFAIL(sums.ContainsKey("Procedure/Func2"));
        WVPASSEQ(sums["Procedure/Func1"].checksums.Count, 1);
        WVPASSEQ(sums["Procedure/Func1"].checksums[0], 0x55F9D9E3);

        WVASSERT(VxExec("drop procedure Func1"));
    }

    [Test, Category("Schemamatic"), Category("GetSchemaChecksums")]
    public void TestTableChecksums()
    {
        try { VxExec("drop table Tab1"); } catch { }
        string query = "CREATE TABLE [Tab1] (\n" + 
            "\t[f1] [int] NOT NULL PRIMARY KEY,\n" +
            "\t[f2] [money] NULL,\n" + 
            "\t[f3] [varchar] (80) NULL);\n\n";
        WVASSERT(VxExec(query));

        VxSchemaChecksums sums;
        sums = dbus.GetChecksums();

        // Three columns gives us three checksums
        WVPASSEQ(sums["Table/Tab1"].checksums.Count, 3);
        WVPASSEQ(sums["Table/Tab1"].checksums[0], 0xE8634548)
        WVPASSEQ(sums["Table/Tab1"].checksums[1], 0xA5F77357)
        WVPASSEQ(sums["Table/Tab1"].checksums[2], 0xE50EE702)

        WVASSERT(VxExec("drop table Tab1"));
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

        WVPASSEQ(sums["Index/Tab1/Index1"].checksums.Count, 1);
        WVPASSEQ(sums["Index/Tab1/Index1"].checksums[0], 0x62781FDD);
        // An index on two columns will include two checksums
        WVPASSEQ(sums["Index/Tab1/Index2"].checksums.Count, 2);
        WVPASSEQ(sums["Index/Tab1/Index2"].checksums[0], 0x603EA184);
        WVPASSEQ(sums["Index/Tab1/Index2"].checksums[1], 0x8FD2C903);

        WVASSERT(VxExec("drop table Tab1"));
    }

    string CreateXmlSchemaQuery()
    {
        return "CREATE XML SCHEMA COLLECTION [dbo].[TestSchema] AS " + 
            "'<xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">" +
             "<xsd:element name=\"Employee\">" + 
              "<xsd:complexType>" + 
               "<xsd:complexContent>" + 
                "<xsd:restriction base=\"xsd:anyType\">" + 
                 "<xsd:sequence>" + 
                  "<xsd:element name=\"SIN\" type=\"xsd:string\"/>" + 
                  "<xsd:element name=\"Name\" type=\"xsd:string\"/>" +
                  "<xsd:element name=\"DateOfBirth\" type=\"xsd:date\"/>" +
                  "<xsd:element name=\"EmployeeType\" type=\"xsd:string\"/>" +
                  "<xsd:element name=\"Salary\" type=\"xsd:long\"/>" +
                 "</xsd:sequence>" +
                "</xsd:restriction>" + 
               "</xsd:complexContent>" + 
              "</xsd:complexType>" +
             "</xsd:element>" +
            "</xsd:schema>'\n";
    }

    [Test, Category("Schemamatic"), Category("GetSchemaChecksums")]
    public void TestXmlSchemaChecksums()
    {
        try { VxExec("drop xml schema collection TestSchema"); } catch { }
        WVASSERT(VxExec(CreateXmlSchemaQuery()));

        VxSchemaChecksums sums;
        sums = dbus.GetChecksums();

        WVPASSEQ(sums["XMLSchema/TestSchema"].checksums.Count, 1);
        WVPASSEQ(sums["XMLSchema/TestSchema"].checksums[0], 0xFA7736B3);

        WVASSERT(VxExec("drop xml schema collection TestSchema"));
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

    // Checks that the schema contains a primary key for tablename, and
    // returns the primary key's name.
    public string CheckForPrimaryKey(VxSchema schema, string tablename)
    {
	string pk_name = null;

        string prefix = "Index/" + tablename + "/";

	// Primary key names are generated and unpredictable.  Just make sure
	// that we only got one back, and that it looks like the right one.
	foreach (string key in schema.Keys)
        {
            Console.WriteLine("Looking at " + key);
	    if (key.StartsWith(prefix + "PK__" + tablename))
	    {
		WVASSERT(pk_name == null)
		pk_name = key.Substring(prefix.Length);
		Console.WriteLine("Found primary key index " + pk_name);
		// Note: don't break here, so we can check there aren't others.
	    }
        }
	WVASSERT(pk_name != null);

	WVASSERT(schema.ContainsKey(prefix + pk_name));
	WVPASSEQ(schema[prefix + pk_name].name, tablename + "/" + pk_name);
	WVPASSEQ(schema[prefix + pk_name].type, "Index");
	WVPASSEQ(schema[prefix + pk_name].encrypted, false);
	string pk_query = String.Format(
	    "ALTER TABLE [{0}] ADD CONSTRAINT [{1}] PRIMARY KEY CLUSTERED\n" +
	    "\t(f1);\n\n", tablename, pk_name);
	WVPASSEQ(schema[prefix + pk_name].text.Length, pk_query.Length);
	WVPASSEQ(schema[prefix + pk_name].text, pk_query);

        return pk_name;
    }

    [Test, Category("Schemamatic"), Category("GetSchema")]
    public void TestGetIndexSchema()
    {
        try { VxExec("drop table Tab1"); } catch { }
        string query = "CREATE TABLE [Tab1] (\n" + 
            "\t[f1] [int] NOT NULL PRIMARY KEY,\n" +
            "\t[f2] [money] NULL,\n" + 
            "\t[f3] [varchar] (80) NULL);\n\n";
        WVASSERT(VxExec(query));

	string idx1q = "CREATE UNIQUE INDEX [Idx1] ON [Tab1] \n" + 
	    "\t(f2,f3 DESC);\n\n";
        WVASSERT(VxExec(idx1q));

        // Check that the query limiting works
	VxSchema schema = dbus.Get("Index/Tab1/Idx1");
	WVPASSEQ(schema.Count, 1);

	WVASSERT(schema.ContainsKey("Index/Tab1/Idx1"));
	WVPASSEQ(schema["Index/Tab1/Idx1"].name, "Tab1/Idx1");
	WVPASSEQ(schema["Index/Tab1/Idx1"].type, "Index");
	WVPASSEQ(schema["Index/Tab1/Idx1"].encrypted, false);
	WVPASSEQ(schema["Index/Tab1/Idx1"].text.Length, idx1q.Length);
	WVPASSEQ(schema["Index/Tab1/Idx1"].text, idx1q);

        // Now get everything, since we don't know the primary key's name
        schema = dbus.Get();
        WVASSERT(schema.Count >= 2);

	WVASSERT(schema.ContainsKey("Index/Tab1/Idx1"));
	WVPASSEQ(schema["Index/Tab1/Idx1"].name, "Tab1/Idx1");
	WVPASSEQ(schema["Index/Tab1/Idx1"].type, "Index");
	WVPASSEQ(schema["Index/Tab1/Idx1"].encrypted, false);
	WVPASSEQ(schema["Index/Tab1/Idx1"].text.Length, idx1q.Length);
	WVPASSEQ(schema["Index/Tab1/Idx1"].text, idx1q);

        CheckForPrimaryKey(schema, "Tab1");

        WVASSERT(VxExec("drop index Tab1.Idx1"));
        WVASSERT(VxExec("drop table Tab1"));
    }

    [Test, Category("Schemamatic"), Category("GetSchema")]
    public void TestGetXmlSchemas()
    {
        try { VxExec("drop xml schema collection TestSchema"); } catch { }
        try { VxExec("drop xml schema collection TestSchema2"); } catch { }

        string query1 = CreateXmlSchemaQuery();
        WVASSERT(VxExec(query1));

	// Make a long XML Schema, to test the 4000-character chunking
        string query2 = @"CREATE XML SCHEMA COLLECTION [dbo].[TestSchema2] AS " + 
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
            "[f1] [int] NOT NULL PRIMARY KEY,\n\t" +
            "[f2] [money] NULL,\n\t" + 
            "[f3] [varchar] (80) NOT NULL,\n\t" +
            "[f4] [varchar] (max) DEFAULT 'Default Value' NULL,\n\t" + 
            "[f5] [decimal] (3,2),\n\t" + 
            "[f6] [bigint] NOT NULL IDENTITY(4,5));\n\n";
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
        try { VxExec("drop index Tab1.Idx1"); } catch { }
        try { VxExec("drop table Tab1"); } catch { }
        try { VxExec("drop table Tab2"); } catch { }
        try { VxExec("drop xml schema collection TestSchema"); } catch { }
        try { VxExec("drop procedure Func1"); } catch { }

        string tab1q = "CREATE TABLE [Tab1] (\n" + 
            "\t[f1] [int] NOT NULL PRIMARY KEY,\n" +
            "\t[f2] [money] NULL,\n" + 
            "\t[f3] [varchar] (80) NULL);\n\n";
        WVASSERT(VxExec(tab1q));

        string tab2q = "CREATE TABLE [Tab2] (\n" + 
            "\t[f4] [binary] (1) NOT NULL);\n\n";
        WVASSERT(VxExec(tab2q));

	string idx1q = "CREATE UNIQUE INDEX [Idx1] ON [Tab1] \n" + 
	    "\t(f2,f3 DESC);\n\n";
        WVASSERT(VxExec(idx1q));

        string msg = "Hello, world, this is Func1!";
        WVASSERT(VxExec("create procedure Func1 as select '" + msg + "'"));

        WVASSERT(VxExec(CreateXmlSchemaQuery()));
        
        VxSchemaChecksums sums = dbus.GetChecksums();

        WVASSERT(sums.ContainsKey("Index/Tab1/Idx1"));
        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVASSERT(sums.ContainsKey("Table/Tab1"));
        WVASSERT(sums.ContainsKey("Table/Tab2"));
        WVASSERT(sums.ContainsKey("XMLSchema/TestSchema"));

        dbus.DropSchema("Index", "Tab1/Idx1");
        dbus.DropSchema("Procedure", "Func1");
        dbus.DropSchema("Table", "Tab2");
        dbus.DropSchema("XMLSchema", "TestSchema");

        sums = dbus.GetChecksums();

        WVASSERT(!sums.ContainsKey("Index/Tab1/Idx1"));
        WVASSERT(!sums.ContainsKey("Procedure/Func1"));
        WVASSERT(sums.ContainsKey("Table/Tab1"));
        WVASSERT(!sums.ContainsKey("Table/Tab2"));
        WVASSERT(!sums.ContainsKey("XMLSchema/TestSchema"));

        try {
            WVEXCEPT(dbus.DropSchema("Procedure", "Func1"));
        } catch (Wv.Test.WvAssertionFailure e) {
            throw e;
        } catch (System.Exception e) {
            // FIXME: This should check for a vx.db.sqlerror
            // rather than any dbus error
            WVPASS(e is DbusError);
            Console.WriteLine(e.ToString());
        }

        try { VxExec("drop index Tab1.Idx1"); } catch { }
        try { VxExec("drop table Tab1"); } catch { }
        try { VxExec("drop table Tab2"); } catch { }
        try { VxExec("drop xml schema collection TestSchema"); } catch { }
        try { VxExec("drop procedure Func1"); } catch { }
    }

    [Test, Category("Schemamatic"), Category("DropSchema")]
    public void TestDropSchemaFromDisk()
    {
        string tmpdir = Path.Combine(Path.GetTempPath(), 
            Path.GetRandomFileName());
        Console.WriteLine("Using temporary directory " + tmpdir);
        Directory.CreateDirectory(tmpdir);
        try
        {
            VxDiskSchema backend = new VxDiskSchema(tmpdir);

            VxSchema schema = new VxSchema();
            schema.Add("Foo", "Table", "Foo contents", false);
            schema.Add("Bar", "Table", "Bar contents", false);
            schema.Add("Func1", "Procedure", "Func1 contents", false);

            VxSchemaChecksums sums = new VxSchemaChecksums();
            sums.Add("Table/Foo", 1);
            sums.Add("Table/Bar", 2);
            sums.Add("Procedure/Func1", 3);

            backend.Put(schema, sums, VxPutOpts.None);

            WVPASS(File.Exists(Path.Combine(tmpdir, "Table/Foo")));
            WVPASS(File.Exists(Path.Combine(tmpdir, "Table/Bar")));
            WVPASS(File.Exists(Path.Combine(tmpdir, "Procedure/Func1")));

            VxSchema newschema = backend.Get(null);
            VxSchemaChecksums newsums = backend.GetChecksums();

            WVPASSEQ(newschema.Count, schema.Count);
            WVPASSEQ(newsums.Count, sums.Count);
            WVPASS(newschema.ContainsKey("Table/Foo"));
            WVPASS(newschema.ContainsKey("Table/Bar"));
            WVPASS(newschema.ContainsKey("Procedure/Func1"));

            backend.DropSchema("Table", "Foo");

            newschema = backend.Get(null);
            newsums = backend.GetChecksums();
            WVPASSEQ(newschema.Count, 2);
            WVPASSEQ(newsums.Count, 2);
            WVPASS(!newschema.ContainsKey("Table/Foo"));
            WVPASS(newschema.ContainsKey("Table/Bar"));
            WVPASS(newschema.ContainsKey("Procedure/Func1"));

            backend.DropSchema("Procedure", "Func1");

            newschema = backend.Get(null);
            newsums = backend.GetChecksums();
            WVPASSEQ(newschema.Count, 1);
            WVPASSEQ(newsums.Count, 1);
            WVPASS(!newschema.ContainsKey("Table/Foo"));
            WVPASS(newschema.ContainsKey("Table/Bar"));
            WVPASS(!newschema.ContainsKey("Procedure/Func1"));
        }
        finally
        {
            Directory.Delete(tmpdir, true);
            WVPASS(!Directory.Exists(tmpdir));
        }
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestPutSchema()
    {
        try { VxExec("drop index Tab1.Idx1"); } catch { }
        try { VxExec("drop table Tab1"); } catch { }
        try { VxExec("drop table Tab2"); } catch { }
        try { VxExec("drop xml schema collection TestSchema"); } catch { }
        try { VxExec("drop procedure Func1"); } catch { }

        string tab1q = "CREATE TABLE [Tab1] (\n\t" + 
            "[f1] [int] NOT NULL,\n\t" +
            "[f2] [money] NULL,\n\t" + 
            "[f3] [varchar] (80) NULL);\n\n";
        VxPutOpts no_opts = VxPutOpts.None;
        WVPASSEQ(VxPutSchema("Table", "Tab1", tab1q, no_opts), null);

	string idx1q = "CREATE UNIQUE INDEX [Idx1] ON [Tab1] \n" + 
	    "\t(f2,f3 DESC);\n\n";
        WVPASSEQ(VxPutSchema("Index", "Tab1/Idx1", idx1q, no_opts), null);

        string msg = "Hello, world, this is Func1!";
        string func1q = "create procedure Func1 as select '" + msg + "'";
        WVPASSEQ(VxPutSchema("Procedure", "Func1", func1q, no_opts), null);

        WVPASSEQ(VxPutSchema("XMLSchema", "TestSchema", 
            CreateXmlSchemaQuery(), no_opts), null);
        
        VxSchema schema = dbus.Get();

        WVASSERT(schema.ContainsKey("Index/Tab1/Idx1"));
        WVPASSEQ(schema["Index/Tab1/Idx1"].name, "Tab1/Idx1");
        WVPASSEQ(schema["Index/Tab1/Idx1"].type, "Index");
        WVPASSEQ(schema["Index/Tab1/Idx1"].text, idx1q);
        WVASSERT(schema.ContainsKey("Procedure/Func1"));
        WVPASSEQ(schema["Procedure/Func1"].name, "Func1");
        WVPASSEQ(schema["Procedure/Func1"].type, "Procedure");
        WVPASSEQ(schema["Procedure/Func1"].text, func1q);
        WVASSERT(schema.ContainsKey("Table/Tab1"));
        WVPASSEQ(schema["Table/Tab1"].name, "Tab1");
        WVPASSEQ(schema["Table/Tab1"].type, "Table");
        WVPASSEQ(schema["Table/Tab1"].text, tab1q);
        WVASSERT(schema.ContainsKey("XMLSchema/TestSchema"));
        WVPASSEQ(schema["XMLSchema/TestSchema"].name, "TestSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].type, "XMLSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].text, CreateXmlSchemaQuery());

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
        WVPASSEQ(schema["Table/Tab1"].text, tab1q);

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

        try { VxExec("drop index Tab1.Idx1"); } catch { }
        try { VxExec("drop table Tab1"); } catch { }
        try { VxExec("drop table Tab2"); } catch { }
        try { VxExec("drop xml schema collection TestSchema"); } catch { }
        try { VxExec("drop procedure Func1"); } catch { }
    }

    [Test, Category("Schemamatic"), Category("SchemaData")]
    public void TestSchemaData()
    {
        try { VxExec("drop table Tab1"); } catch { }

        string tab1q = "CREATE TABLE [Tab1] (\n\t" + 
            "[f1] [int] NOT NULL,\n\t" +
            "[f2] [money] NULL,\n\t" + 
            "[f3] [varchar] (80) NULL);\n\n";
        VxPutOpts no_opts = VxPutOpts.None;
        WVPASSEQ(VxPutSchema("Table", "Tab1", tab1q, no_opts), null);

        List<string> inserts = new List<string>();
        for (int ii = 0; ii < 22; ii++)
        {
            inserts.Add(String.Format("INSERT INTO Tab1 ([f1],[f2],[f3]) " + 
                "VALUES ({0},{1},'{2}');\n", 
                ii, ii + ".3400", "Hi" + ii));
        }

        foreach (string ins in inserts)
            WVASSERT(VxExec(ins));

        WVPASSEQ(dbus.GetSchemaData("Tab1"), inserts.Join(""));

        try { VxExec("drop table Tab1"); } catch { }

        try {
            WVEXCEPT(dbus.GetSchemaData("Tab1"));
	} catch (Wv.Test.WvAssertionFailure e) {
	    throw e;
	} catch (System.Exception e) {
            // FIXME: This should check for a vx.db.sqlerror
            // rather than any dbus error
	    WVPASS(e is DbusError);
            Console.WriteLine(e.ToString());
	}

        WVPASSEQ(VxPutSchema("Table", "Tab1", tab1q, no_opts), null);

        WVPASSEQ(dbus.GetSchemaData("Tab1"), "");

        dbus.PutSchemaData("Tab1", inserts.Join(""));
        WVPASSEQ(dbus.GetSchemaData("Tab1"), inserts.Join(""));
    }

    [Test, Category("Schemamatic"), Category("DiskBackend")]
    public void TestExportEmptySchema()
    {
        string tmpdir = Path.Combine(Path.GetTempPath(), 
            Path.GetRandomFileName());
        Console.WriteLine("Using temporary directory " + tmpdir);

        Directory.CreateDirectory(tmpdir);
        try 
        {
            VxSchema schema = new VxSchema();
            VxSchemaChecksums sums = new VxSchemaChecksums();

            // Check that exporting an empty schema doesn't touch anything.
            VxDiskSchema backend = new VxDiskSchema(tmpdir);
            backend.Put(schema, sums, VxPutOpts.None);
            WVPASSEQ(Directory.GetDirectories(tmpdir).Length, 0);
            WVPASSEQ(Directory.GetFiles(tmpdir).Length, 0);
        }
        finally
        {
            Directory.Delete(tmpdir);
            WVASSERT(!Directory.Exists(tmpdir));
        }
    }

    private void CheckExportedFileContents(string filename, string header, 
        string text)
    {
        WVPASS(File.Exists(filename));
        using (StreamReader sr = new StreamReader(filename))
        {
            WVPASSEQ(sr.ReadLine(), header);
            string line;
            StringBuilder sb = new StringBuilder();
            while ((line = sr.ReadLine()) != null)
                sb.Append(line + "\n");
            WVPASSEQ(sb.ToString(), text);
        }
    }

    private void VerifyExportedSchema(string exportdir, VxSchema schema, 
        VxSchemaChecksums sums, string func1q, string tab1q, string tab2q,
        string idx1q, string xmlq, int backupnum)
    {
        DirectoryInfo dirinfo = new DirectoryInfo(exportdir);

        int filemultiplier = backupnum + 1;
        string suffix = backupnum == 0 ? "" : "-" + backupnum;

        string procdir = Path.Combine(exportdir, "Procedure");
        string idxdir = Path.Combine(exportdir, "Index");
        string tabdir = Path.Combine(exportdir, "Table");
        string xmldir = Path.Combine(exportdir, "XMLSchema");

        WVPASSEQ(dirinfo.GetDirectories().Length, 4);
        WVPASSEQ(dirinfo.GetFiles().Length, 0);
        WVPASS(Directory.Exists(procdir));
        WVPASS(Directory.Exists(idxdir));
        WVPASS(Directory.Exists(tabdir));
        WVPASS(Directory.Exists(xmldir));

        // Procedures
        WVPASSEQ(Directory.GetDirectories(procdir).Length, 0);
        WVPASSEQ(Directory.GetFiles(procdir).Length, 1 * filemultiplier);
        string func1file = Path.Combine(procdir, "Func1" + suffix);
        CheckExportedFileContents(func1file, 
            "!!SCHEMAMATIC 2AE46AC0748AEDE839FB9CD167EA1180 D983A305",
            func1q);

        // Indexes
        WVPASSEQ(Directory.GetDirectories(idxdir).Length, 1);
        WVPASSEQ(Directory.GetFiles(idxdir).Length, 0);

        string tab1idxdir = Path.Combine(idxdir, "Tab1");
        WVPASS(Directory.Exists(tab1idxdir));
        WVPASSEQ(Directory.GetDirectories(tab1idxdir).Length, 0);
        WVPASSEQ(Directory.GetFiles(tab1idxdir).Length, 2 * filemultiplier);

        string idx1file = Path.Combine(tab1idxdir, "Idx1" + suffix);
        CheckExportedFileContents(idx1file, 
            "!!SCHEMAMATIC BE6095FA7C7B1C9BA3D3DA2F1D94FCBE 1D32C7EA 968DBEDC", 
            idx1q);

        string pk_name = CheckForPrimaryKey(schema, "Tab1");
        string pk_file = Path.Combine(tab1idxdir, pk_name + suffix);
        string pk_query = String.Format(
            "ALTER TABLE [Tab1] ADD CONSTRAINT [{0}] " + 
            "PRIMARY KEY CLUSTERED\n" +
            "\t(f1);\n\n", pk_name);

        // We can't know the primary key's MD5 ahead of time, so compute
        // it ourselves.
        byte[] md5 = MD5.Create().ComputeHash(Encoding.UTF8.GetBytes(
            pk_query));
        string md5str = md5.ToHex();

        CheckExportedFileContents(pk_file, 
            String.Format("!!SCHEMAMATIC {0} {1}", 
                md5str, sums["Index/Tab1/" + pk_name].GetSumString()),
            pk_query);

        // Tables
        WVPASSEQ(Directory.GetDirectories(tabdir).Length, 0);
        WVPASSEQ(Directory.GetFiles(tabdir).Length, 2 * filemultiplier);

        string tab1file = Path.Combine(tabdir, "Tab1" + suffix);
        string tab2file = Path.Combine(tabdir, "Tab2" + suffix);

        WVPASS(File.Exists(tab1file));
        CheckExportedFileContents(tab1file, 
            "!!SCHEMAMATIC 3D05ABB172361D5BDC19DE2437C58F7E " + 
                sums["Table/Tab1"].GetSumString(),
            tab1q.Replace(" PRIMARY KEY", ""));

        WVPASS(File.Exists(tab2file));
        CheckExportedFileContents(tab2file, 
            "!!SCHEMAMATIC 436EFDE94964E924CB0CCEDB96970AFF " + 
            sums["Table/Tab2"].GetSumString(), tab2q);

        // XML Schemas
        WVPASSEQ(Directory.GetDirectories(xmldir).Length, 0);
        WVPASSEQ(Directory.GetFiles(xmldir).Length, 1 * filemultiplier);

        string testschemafile = Path.Combine(xmldir, "TestSchema" + suffix);
        WVPASS(File.Exists(testschemafile));
        CheckExportedFileContents(testschemafile, 
            "!!SCHEMAMATIC 3D84628C4C6A7805CB9BF97B432D2268 FA7736B3", 
            xmlq);
    }

    [Test, Category("Schemamatic"), Category("DiskBackend")]
    public void TestExportSchema()
    {
        try { VxExec("drop index Tab1.Idx1"); } catch { }
        try { VxExec("drop table Tab1"); } catch { }
        try { VxExec("drop table Tab2"); } catch { }
        try { VxExec("drop xml schema collection TestSchema"); } catch { }
        try { VxExec("drop procedure Func1"); } catch { }

        string tab1q = "CREATE TABLE [Tab1] (\n" + 
            "\t[f1] [int] NOT NULL PRIMARY KEY,\n" +
            "\t[f2] [money] NULL,\n" + 
            "\t[f3] [varchar] (80) NULL);\n\n";
        WVASSERT(VxExec(tab1q));

        string tab2q = "CREATE TABLE [Tab2] (\n" + 
            "\t[f4] [binary] (1) NOT NULL);\n\n";
        WVASSERT(VxExec(tab2q));

	string idx1q = "CREATE UNIQUE INDEX [Idx1] ON [Tab1] \n" + 
	    "\t(f2,f3 DESC);\n\n";
        WVASSERT(VxExec(idx1q));

        string msg = "Hello, world, this is Func1!";
        string func1q = "create procedure Func1 as select '" + msg + "'\n";
        WVASSERT(VxExec(func1q));

        string xmlq = CreateXmlSchemaQuery();
        WVASSERT(VxExec(xmlq));
        
        string tmpdir = Path.Combine(Path.GetTempPath(), 
            Path.GetRandomFileName());
        Console.WriteLine("Using temporary directory " + tmpdir);

        DirectoryInfo tmpdirinfo = new DirectoryInfo(tmpdir);
        try
        {
            tmpdirinfo.Create();

            // Check that having mangled checksums fails
            VxSchema schema = dbus.Get();
            VxSchemaChecksums sums = new VxSchemaChecksums();

            VxDiskSchema disk = new VxDiskSchema(tmpdir);
            try {
                WVEXCEPT(disk.Put(schema, sums, VxPutOpts.None));
            } catch (Wv.Test.WvAssertionFailure e) {
                throw e;
            } catch (System.Exception e) {
                WVPASS(e is ArgumentException);
                Console.WriteLine(e.ToString());
            }

            // Check that the normal exporting works.
            sums = dbus.GetChecksums();
            disk.Put(schema, sums, VxPutOpts.None);

            int backup_generation = 0;
            VerifyExportedSchema(tmpdir, schema, sums, 
                func1q, tab1q, tab2q, idx1q, xmlq, backup_generation);

            // Check that we read back the same stuff
            VxSchema schemafromdisk = disk.Get(null);
            foreach (KeyValuePair<string,VxSchemaElement> p in schema)
            {
                WVPASSEQ(schemafromdisk[p.Key].type, p.Value.type);
                WVPASSEQ(schemafromdisk[p.Key].name, p.Value.name);
                WVPASSEQ(schemafromdisk[p.Key].text, p.Value.text);
                WVPASSEQ(schemafromdisk[p.Key].encrypted, p.Value.encrypted);
            }

            VxSchemaChecksums sumsfromdisk = disk.GetChecksums();

            WVPASSEQ(sumsfromdisk.Count, sums.Count);
            foreach (KeyValuePair<string,VxSchemaChecksum> p in sums)
            {
                WVPASSEQ(sumsfromdisk[p.Key].GetSumString(), 
                    p.Value.GetSumString());
            }

            // Doing it twice doesn't change anything.
            disk.Put(schema, sums, VxPutOpts.None);

            VerifyExportedSchema(tmpdir, schema, sums, 
                func1q, tab1q, tab2q, idx1q, xmlq, backup_generation);

            // Check backup mode
            disk.Put(schema, sums, VxPutOpts.IsBackup);
            backup_generation++;

            VerifyExportedSchema(tmpdir, schema, sums, 
                func1q, tab1q, tab2q, idx1q, xmlq, backup_generation);

            // Check backup mode again
            disk.Put(schema, sums, VxPutOpts.IsBackup);
            backup_generation++;

            VerifyExportedSchema(tmpdir, schema, sums, 
                func1q, tab1q, tab2q, idx1q, xmlq, backup_generation);
        }
        finally
        {
            tmpdirinfo.Delete(true);
            WVASSERT(!tmpdirinfo.Exists);
        }
    }

    [Test, Category("Schemamatic"), Category("DiskBackend")]
    public void TestReadChecksums()
    {
        try { VxExec("drop index Tab1.Idx1"); } catch { }
        try { VxExec("drop table Tab1"); } catch { }
        try { VxExec("drop table Tab2"); } catch { }
        try { VxExec("drop xml schema collection TestSchema"); } catch { }
        try { VxExec("drop procedure Func1"); } catch { }

        string tab1q = "CREATE TABLE [Tab1] (\n" + 
            "\t[f1] [int] NOT NULL PRIMARY KEY,\n" +
            "\t[f2] [money] NULL,\n" + 
            "\t[f3] [varchar] (80) NULL);\n\n";
        WVASSERT(VxExec(tab1q));

        string tab2q = "CREATE TABLE [Tab2] (\n" + 
            "\t[f4] [binary] (1) NOT NULL);\n\n";
        WVASSERT(VxExec(tab2q));

	string idx1q = "CREATE UNIQUE INDEX [Idx1] ON [Tab1] \n" + 
	    "\t(f2,f3 DESC);\n\n";
        WVASSERT(VxExec(idx1q));

        string msg = "Hello, world, this is Func1!";
        string func1q = "create procedure Func1 as select '" + msg + "'\n";
        WVASSERT(VxExec(func1q));

        string xmlq = CreateXmlSchemaQuery();
        WVASSERT(VxExec(xmlq));
        
        string tmpdir = Path.Combine(Path.GetTempPath(), 
            Path.GetRandomFileName());
        Console.WriteLine("Using temporary directory " + tmpdir);

        DirectoryInfo tmpdirinfo = new DirectoryInfo(tmpdir);
        try
        {
            tmpdirinfo.Create();

            VxSchema schema = dbus.Get();
            VxSchemaChecksums sums = dbus.GetChecksums();
            VxDiskSchema backend = new VxDiskSchema(tmpdir);
            backend.Put(schema, sums, VxPutOpts.None);

            VxSchemaChecksums fromdisk = backend.GetChecksums();

            foreach (KeyValuePair<string, VxSchemaChecksum> p in sums)
            {
                WVPASSEQ(p.Value.GetSumString(), fromdisk[p.Key].GetSumString());
            }
            WVPASSEQ(sums.Count, fromdisk.Count);

            // Test that changing a file invalidates its checksums, and that
            // we skip directories named "DATA"
            using (StreamWriter sw = File.AppendText(
                wv.PathCombine(tmpdir, "Table", "Tab1")))
            {
                sw.WriteLine("Ooga Booga");
            }

            Directory.CreateDirectory(Path.Combine(tmpdir, "DATA"));
            File.WriteAllText(wv.PathCombine(tmpdir, "DATA", "Decoy"),
                "Decoy file, shouldn't have checksums");

            VxSchemaChecksums mangled = backend.GetChecksums();

            // Check that the decoy file didn't get read
            WVFAIL(mangled.ContainsKey("DATA/Decoy"));

            // Check that the mangled checksums exist, but are empty.
            WVASSERT(mangled.ContainsKey("Table/Tab1"));
            WVASSERT(mangled["Table/Tab1"].GetSumString() != 
                sums["Table/Tab1"].GetSumString());
            WVPASSEQ(mangled["Table/Tab1"].GetSumString(), "");

            // Check that everything else is still sensible
            foreach (KeyValuePair<string, VxSchemaChecksum> p in sums)
            {
                if (p.Key != "Table/Tab1")
                    WVPASSEQ(p.Value.GetSumString(), 
                        mangled[p.Key].GetSumString());
            }
        }
        finally
        {
            tmpdirinfo.Delete(true);
            WVASSERT(!tmpdirinfo.Exists);
        }
    }

    [Test, Category("Schemamatic"), Category("VxSchemaDiff")]
    public void TestChecksumDiff()
    {
        VxSchemaChecksums srcsums = new VxSchemaChecksums();
        VxSchemaChecksums goalsums = new VxSchemaChecksums();
        VxSchemaChecksums emptysums = new VxSchemaChecksums();

        srcsums.Add("XMLSchema/secondxml", 2);
        srcsums.Add("XMLSchema/firstxml", 1);
        srcsums.Add("Index/Tab1/ConflictIndex", 3);
        srcsums.Add("Table/HarmonyTable", 6);

        goalsums.Add("Table/NewTable", 3);
        goalsums.Add("Procedure/NewFunc", 4);
        goalsums.Add("Index/Tab1/ConflictIndex", 5);
        goalsums.Add("Table/HarmonyTable", 6);

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
        try { VxExec("drop index Tab1.Idx1"); } catch { }
        try { VxExec("drop table Tab1"); } catch { }
        try { VxExec("drop table Tab2"); } catch { }
        try { VxExec("drop xml schema collection TestSchema"); } catch { }
        try { VxExec("drop procedure Func1"); } catch { }

        string tab1q = "CREATE TABLE [Tab1] (\n" + 
            "\t[f1] [int] NOT NULL PRIMARY KEY,\n" +
            "\t[f2] [money] NULL,\n" + 
            "\t[f3] [varchar] (80) NULL);\n\n";
        WVASSERT(VxExec(tab1q));

        string tab2q = "CREATE TABLE [Tab2] (\n" + 
            "\t[f4] [binary] (1) NOT NULL);\n\n";
        WVASSERT(VxExec(tab2q));

	string idx1q = "CREATE UNIQUE INDEX [Idx1] ON [Tab1] \n" + 
	    "\t(f2,f3 DESC);\n\n";
        WVASSERT(VxExec(idx1q));

        string msg1 = "Hello, world, this is Func1!";
        string msg2 = "Hello, world, this used to be Func1!";
        string func1q = "create procedure Func1 as select '" + msg1 + "'\n";
        string func1q2 = "create procedure Func1 as select '" + msg2 + "'\n";
        WVASSERT(VxExec(func1q));

        string xmlq = CreateXmlSchemaQuery();
        WVASSERT(VxExec(xmlq));
        
        string tmpdir = Path.Combine(Path.GetTempPath(), 
            Path.GetRandomFileName());
        Console.WriteLine("Using temporary directory " + tmpdir);

        DirectoryInfo tmpdirinfo = new DirectoryInfo(tmpdir);
        try
        {
            tmpdirinfo.Create();

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
            newsums["Procedure/Func1"].checksums.Clear();
            newsums["Procedure/Func1"].checksums.Add(123);
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
            WVPASSEQ(diffschema["Index/Tab1/Idx1"].text, idx1q);
            WVPASSEQ(diffschema["Procedure/Func1"].type, "Procedure");
            WVPASSEQ(diffschema["Procedure/Func1"].name, "Func1");
            WVPASSEQ(diffschema["Procedure/Func1"].text, func1q2);

            VxSchemaErrors errs = backend.Put(diffschema, diffsums, 
                VxPutOpts.None);
            WVPASSEQ(errs.Count, 0);

            VxSchema updated = backend.Get(null);
            WVASSERT(!updated.ContainsKey("XMLSchema/TestSchema"));
            WVPASSEQ(updated["Index/Tab1/Idx1"].text, 
                newschema["Index/Tab1/Idx1"].text);
            WVPASSEQ(updated["Procedure/Func1"].text, 
                newschema["Procedure/Func1"].text);
            WVPASSEQ(updated["Table/Tab1"].text, newschema["Table/Tab1"].text);
        }
        finally
        {
            tmpdirinfo.Delete(true);
            WVASSERT(!tmpdirinfo.Exists);
        }

        try { VxExec("drop index Tab1.Idx1"); } catch { }
        try { VxExec("drop table Tab1"); } catch { }
        try { VxExec("drop table Tab2"); } catch { }
        try { VxExec("drop xml schema collection TestSchema"); } catch { }
        try { VxExec("drop procedure Func1"); } catch { }
    }

    [Test, Category("Schemamatic"), Category("PutSchema")]
    public void TestApplySchemaDiff()
    {
        Console.WriteLine("Testing applying diffs through DBus");
        TestApplySchemaDiff(dbus);

        Console.WriteLine("Testing applying diffs to the disk");

        string tmpdir = Path.Combine(Path.GetTempPath(), 
            Path.GetRandomFileName());
        Console.WriteLine("Using temporary directory " + tmpdir);
        Directory.CreateDirectory(tmpdir);
        try
        {
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
        try { VxExec("drop table Tab1"); } catch { }
        try { VxExec("drop table Tab2"); } catch { }

        string tab1q = "CREATE TABLE [Tab1] (\n" + 
            "\t[f1] [int] NOT NULL PRIMARY KEY,\n" +
            "\t[f2] [money] NULL,\n" + 
            "\t[f3] [varchar] (80) NULL);\n\n";
        WVASSERT(VxExec(tab1q));

        string tab2q = "CREATE TABLE [Tab2] (\n" + 
            "\t[f4] [binary] (1) NOT NULL);\n\n";
        WVASSERT(VxExec(tab2q));

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

        try { VxExec("drop table Tab1"); } catch { }
        try { VxExec("drop table Tab2"); } catch { }
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
        schema.Add("View1", "View", view1q, false);
        schema.Add("View2", "View", view2q, false);
        schema.Add("View3", "View", view3q, false);
        schema.Add("View4", "View", view4q, false);

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

    public static void Main()
    {
        WvTest.DoMain();
    }
}
