#include "wvtest.cs.h"
// Test the Schemamatic functions that live in the Versaplex daemon.

using System;
using System.IO;
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
    Exception GetDbusException(Message reply)
    {
        object errname;
        if (!reply.Header.Fields.TryGetValue(FieldCode.ErrorName,
                    out errname))
            return new Exception("D-Bus error received but no error name "
                    +"given");

        object errsig;
        if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                    out errsig) || errsig.ToString() != "s")
            return new DbusError(errname.ToString());

        MessageReader mr = new MessageReader(reply);

        object errmsg;
        mr.GetValue(typeof(string), out errmsg);

        return new DbusError(errname.ToString() + ": " + errmsg.ToString());
    }

    VxSchemaChecksums VxGetSchemaChecksums()
    {
	Console.WriteLine(" + VxGetSchemaChecksums");

        Message call = CreateMethodCall("GetSchemaChecksums", "");

        Message reply = call.Connection.SendWithReplyAndBlock(call);
        Console.WriteLine("Got reply");

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        {
            object replysig;
            if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out replysig))
                throw new Exception("D-Bus reply had no signature");

            if (replysig == null || replysig.ToString() != "a(sat)")
                throw new Exception("D-Bus reply had invalid signature: " +
                    replysig);

            MessageReader reader = new MessageReader(reply);
            VxSchemaChecksums sums = new VxSchemaChecksums(reader);
            return sums;
        }
        case MessageType.Error:
            throw GetDbusException(reply);
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    VxSchema VxGetSchema(params string[] names)
    {
	Console.WriteLine(" + VxGetSchema");

        Message call = CreateMethodCall("GetSchema", "as");

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        writer.Write(typeof(string[]), (Array)names);
        call.Body = writer.ToArray();

        Message reply = call.Connection.SendWithReplyAndBlock(call);
        Console.WriteLine("Got reply");

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        {
            object replysig;
            if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out replysig))
                throw new Exception("D-Bus reply had no signature");

            if (replysig == null || replysig.ToString() != "a(sssy)")
                throw new Exception("D-Bus reply had invalid signature: " +
                    replysig);

            MessageReader reader = new MessageReader(reply);
            VxSchema schema = new VxSchema(reader);
            return schema;
        }
        case MessageType.Error:
            throw GetDbusException(reply);
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }
    
    void VxDropSchema(string type, string name)
    {
	Console.WriteLine(" + VxDropSchema");

        Message call = CreateMethodCall("DropSchema", "ss");

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        writer.Write(typeof(string), type);
        writer.Write(typeof(string), name);
        call.Body = writer.ToArray();

        Message reply = call.Connection.SendWithReplyAndBlock(call);
        Console.WriteLine("Got reply");

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        {
            object replysig;
            if (reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out replysig))
                throw new Exception("D-Bus reply had unexpected signature" + 
                    replysig);

            return;
        }
        case MessageType.Error:
            throw GetDbusException(reply);
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    void VxPutSchema(string type, string name, string text, bool destructive)
    {
	Console.WriteLine(" + VxPutSchema");

        Message call = CreateMethodCall("PutSchema", "sssy");

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        writer.Write(typeof(string), type);
        writer.Write(typeof(string), name);
        writer.Write(typeof(string), text);
        writer.Write(typeof(byte), destructive ? (byte)1 : (byte)0);
        call.Body = writer.ToArray();

        Message reply = call.Connection.SendWithReplyAndBlock(call);
        Console.WriteLine("Got reply");

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        {
            object replysig;
            if (reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out replysig))
                throw new Exception("D-Bus reply had unexpected signature" + 
                    replysig);

            return;
        }
        case MessageType.Error:
            throw GetDbusException(reply);
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    string VxGetSchemaData(string tablename)
    {
	Console.WriteLine(" + VxGetSchemaData");

        Message call = CreateMethodCall("GetSchemaData", "s");

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        writer.Write(typeof(string), tablename);
        call.Body = writer.ToArray();

        Message reply = call.Connection.SendWithReplyAndBlock(call);
        Console.WriteLine("Got reply");

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        {
            object replysig;
            if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out replysig))
                throw new Exception("D-Bus reply had no signature");

            if (replysig == null || replysig.ToString() != "s")
                throw new Exception("D-Bus reply had invalid signature: " +
                    replysig);

            MessageReader reader = new MessageReader(reply);
            string schemadata;
            reader.GetValue(out schemadata);
            return schemadata;
        }
        case MessageType.Error:
            throw GetDbusException(reply);
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    void VxPutSchemaData(string tablename, string text)
    {
	Console.WriteLine(" + VxPutSchemaData");

        Message call = CreateMethodCall("PutSchemaData", "ss");

        MessageWriter writer = new MessageWriter(Connection.NativeEndianness);

        writer.Write(tablename);
        writer.Write(text);
        call.Body = writer.ToArray();

        Message reply = call.Connection.SendWithReplyAndBlock(call);
        Console.WriteLine("Got reply");

        switch (reply.Header.MessageType) {
        case MessageType.MethodReturn:
        {
            object replysig;
            if (reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out replysig))
                throw new Exception("D-Bus reply had unexpected signature" + 
                    replysig);

            return;
        }
        case MessageType.Error:
            throw GetDbusException(reply);
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    [Test, Category("Schemamatic"), Category("GetSchemaChecksums")]
    public void TestProcedureChecksums()
    {
        try { VxExec("drop procedure Func1"); } catch { }
        try { VxExec("drop procedure Func2"); } catch { }

        VxSchemaChecksums sums;
        sums = VxGetSchemaChecksums();
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

        sums = VxGetSchemaChecksums();

        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVPASSEQ(sums["Procedure/Func1"].checksums.Count, 1);
        WVPASSEQ(sums["Procedure/Func1"].checksums[0], 0x55F9D9E3);

        WVASSERT(VxExec("create procedure Func2 with encryption as select '" + 
            msg2 + "'"));

        WVASSERT(VxScalar("exec Func2", out outmsg));
        WVPASSEQ(msg2, (string)outmsg);

        sums = VxGetSchemaChecksums();
        //WVPASSEQ(sums.Count, 2);

        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVASSERT(sums.ContainsKey("Procedure-Encrypted/Func2"));
        WVPASSEQ(sums["Procedure/Func1"].checksums.Count, 1);
        WVPASSEQ(sums["Procedure-Encrypted/Func2"].checksums.Count, 1);
        WVPASSEQ(sums["Procedure/Func1"].checksums[0], 0x55F9D9E3);
        WVPASSEQ(sums["Procedure-Encrypted/Func2"].checksums[0], 0x458D4283);

        WVASSERT(VxExec("drop procedure Func2"));

        sums = VxGetSchemaChecksums();
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
        sums = VxGetSchemaChecksums();

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
        sums = VxGetSchemaChecksums();

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
        sums = VxGetSchemaChecksums();

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
        VxSchema schema = VxGetSchema("Procedure/Func1é");
        WVPASSEQ(schema.Count, 1);

        WVASSERT(schema.ContainsKey("Procedure/Func1!"));
        WVPASSEQ(schema["Procedure/Func1!"].name, "Func1!");
        WVPASSEQ(schema["Procedure/Func1!"].type, "Procedure");
        WVPASSEQ(schema["Procedure/Func1!"].encrypted, false);
        WVPASSEQ(schema["Procedure/Func1!"].text, query1);

        schema = VxGetSchema("Func1é");
        WVPASSEQ(schema.Count, 1);

        WVASSERT(schema.ContainsKey("Procedure/Func1!"));
        WVPASSEQ(schema["Procedure/Func1!"].name, "Func1!");
        WVPASSEQ(schema["Procedure/Func1!"].type, "Procedure");
        WVPASSEQ(schema["Procedure/Func1!"].encrypted, false);
        WVPASSEQ(schema["Procedure/Func1!"].text, query1);

        // Also check that unlimited queries get everything
        schema = VxGetSchema();
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
	VxSchema schema = VxGetSchema("Index/Tab1/Idx1");
	WVPASSEQ(schema.Count, 1);

	WVASSERT(schema.ContainsKey("Index/Tab1/Idx1"));
	WVPASSEQ(schema["Index/Tab1/Idx1"].name, "Tab1/Idx1");
	WVPASSEQ(schema["Index/Tab1/Idx1"].type, "Index");
	WVPASSEQ(schema["Index/Tab1/Idx1"].encrypted, false);
	WVPASSEQ(schema["Index/Tab1/Idx1"].text.Length, idx1q.Length);
	WVPASSEQ(schema["Index/Tab1/Idx1"].text, idx1q);

        // Now get everything, since we don't know the primary key's name
        schema = VxGetSchema();
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
	VxSchema schema = VxGetSchema("TestSchema");
        WVPASSEQ(schema.Count, 1);

        WVASSERT(schema.ContainsKey("XMLSchema/TestSchema"));
        WVPASSEQ(schema["XMLSchema/TestSchema"].name, "TestSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].type, "XMLSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].encrypted, false);
        WVPASSEQ(schema["XMLSchema/TestSchema"].text, query1);

	schema = VxGetSchema("XMLSchema/TestSchema");
        WVPASSEQ(schema.Count, 1);

        WVASSERT(schema.ContainsKey("XMLSchema/TestSchema"));
        WVPASSEQ(schema["XMLSchema/TestSchema"].name, "TestSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].type, "XMLSchema");
        WVPASSEQ(schema["XMLSchema/TestSchema"].encrypted, false);
        WVPASSEQ(schema["XMLSchema/TestSchema"].text, query1);

        // Also check that unlimited queries get everything
	schema = VxGetSchema();
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

        VxSchema schema = VxGetSchema();
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
        
        VxSchemaChecksums sums = VxGetSchemaChecksums();

        WVASSERT(sums.ContainsKey("Index/Tab1/Idx1"));
        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVASSERT(sums.ContainsKey("Table/Tab1"));
        WVASSERT(sums.ContainsKey("Table/Tab2"));
        WVASSERT(sums.ContainsKey("XMLSchema/TestSchema"));

        VxDropSchema("Index", "Tab1/Idx1");
        VxDropSchema("Procedure", "Func1");
        VxDropSchema("Table", "Tab2");
        VxDropSchema("XMLSchema", "TestSchema");

        sums = VxGetSchemaChecksums();

        WVASSERT(!sums.ContainsKey("Index/Tab1/Idx1"));
        WVASSERT(!sums.ContainsKey("Procedure/Func1"));
        WVASSERT(sums.ContainsKey("Table/Tab1"));
        WVASSERT(!sums.ContainsKey("Table/Tab2"));
        WVASSERT(!sums.ContainsKey("XMLSchema/TestSchema"));

        try {
            WVEXCEPT(VxDropSchema("Procedure", "Func1"));
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
        VxPutSchema("Table", "Tab1", tab1q, false);

	string idx1q = "CREATE UNIQUE INDEX [Idx1] ON [Tab1] \n" + 
	    "\t(f2,f3 DESC);\n\n";
        VxPutSchema("Index", "Tab1/Idx1", idx1q, false);

        string msg = "Hello, world, this is Func1!";
        string func1q = "create procedure Func1 as select '" + msg + "'";
        VxPutSchema("Procedure", "Func1", func1q, false);

        VxPutSchema("XMLSchema", "TestSchema", CreateXmlSchemaQuery(), false);
        
        VxSchema schema = VxGetSchema();

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

        try {
            WVEXCEPT(VxPutSchema("Table", "Tab1", tab1q2, false));
	} catch (Wv.Test.WvAssertionFailure e) {
	    throw e;
	} catch (System.Exception e) {
            // FIXME: This should check for a vx.db.sqlerror
            // rather than any dbus error
	    WVPASS(e is DbusError);
            Console.WriteLine(e.ToString());
	}

        schema = VxGetSchema("Table/Tab1");
        WVPASSEQ(schema["Table/Tab1"].name, "Tab1");
        WVPASSEQ(schema["Table/Tab1"].type, "Table");
        WVPASSEQ(schema["Table/Tab1"].text, tab1q);

        VxPutSchema("Table", "Tab1", tab1q2, true);

        schema = VxGetSchema("Table/Tab1");
        WVPASSEQ(schema["Table/Tab1"].name, "Tab1");
        WVPASSEQ(schema["Table/Tab1"].type, "Table");
        WVPASSEQ(schema["Table/Tab1"].text, tab1q2);

        string msg2 = "This is definitely not the Func1 you thought you knew.";
        string func1q2 = "create procedure Func1 as select '" + msg2 + "'";
        VxPutSchema("Procedure", "Func1", func1q2, false);

        schema = VxGetSchema("Procedure/Func1");
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
        VxPutSchema("Table", "Tab1", tab1q, false);

        List<string> inserts = new List<string>();
        for (int ii = 0; ii < 22; ii++)
        {
            inserts.Add(String.Format("INSERT INTO Tab1 ([f1],[f2],[f3]) " + 
                "VALUES ({0},{1},'{2}');\n", 
                ii, ii + ".3400", "Hi" + ii));
        }

        foreach (string ins in inserts)
            WVASSERT(VxExec(ins));

        WVPASSEQ(VxGetSchemaData("Tab1"), inserts.Join(""));

        try { VxExec("drop table Tab1"); } catch { }

        try {
            WVEXCEPT(VxGetSchemaData("Tab1"));
	} catch (Wv.Test.WvAssertionFailure e) {
	    throw e;
	} catch (System.Exception e) {
            // FIXME: This should check for a vx.db.sqlerror
            // rather than any dbus error
	    WVPASS(e is DbusError);
            Console.WriteLine(e.ToString());
	}

        VxPutSchema("Table", "Tab1", tab1q, false);

        WVPASSEQ(VxGetSchemaData("Tab1"), "");

        VxPutSchemaData("Tab1", inserts.Join(""));
        WVPASSEQ(VxGetSchemaData("Tab1"), inserts.Join(""));
    }

    [Test, Category("Schemamatic"), Category("SchemaExport")]
    public void TestExportEmptySchema()
    {
        string tmpdir = Path.Combine(Path.GetTempPath(), 
            Path.GetRandomFileName());
        Console.WriteLine("Using temporary directory " + tmpdir);

        Directory.CreateDirectory(tmpdir);
        VxSchema schema = new VxSchema();
        VxSchemaChecksums sums = new VxSchemaChecksums();

        // Check that exporting an empty schema doesn't touch anything.
        schema.ExportSchema(tmpdir, sums, false);
        WVPASSEQ(Directory.GetDirectories(tmpdir).Length, 0);
        WVPASSEQ(Directory.GetFiles(tmpdir).Length, 0);

        Directory.Delete(tmpdir);
        WVASSERT(!Directory.Exists(tmpdir));
    }

    private void CheckExportedFileContents(string filename, string header, string text)
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

    [Test, Category("Schemamatic"), Category("SchemaExport")]
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
            VxSchema schema = VxGetSchema();
            VxSchemaChecksums sums = new VxSchemaChecksums();

            try {
                WVEXCEPT(schema.ExportSchema(tmpdir, sums, false));
            } catch (Wv.Test.WvAssertionFailure e) {
                throw e;
            } catch (System.Exception e) {
                WVPASS(e is ArgumentException);
                Console.WriteLine(e.ToString());
            }

            // Check that the normal exporting works.
            sums = VxGetSchemaChecksums();
            schema.ExportSchema(tmpdir, sums, false);

            int backup_generation = 0;
            VerifyExportedSchema(tmpdir, schema, sums, 
                func1q, tab1q, tab2q, idx1q, xmlq, backup_generation);

            // Doing it twice doesn't change anything.
            schema.ExportSchema(tmpdir, sums, false);

            VerifyExportedSchema(tmpdir, schema, sums, 
                func1q, tab1q, tab2q, idx1q, xmlq, backup_generation);

            // Check backup mode
            schema.ExportSchema(tmpdir, sums, true);
            backup_generation++;

            VerifyExportedSchema(tmpdir, schema, sums, 
                func1q, tab1q, tab2q, idx1q, xmlq, backup_generation);

            // Check backup mode again
            schema.ExportSchema(tmpdir, sums, true);
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

    [Test, Category("Schemamatic"), Category("ReadChecksums")]
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

            VxSchema schema = VxGetSchema();
            VxSchemaChecksums sums = VxGetSchemaChecksums();
            schema.ExportSchema(tmpdir, sums, false);

            VxSchemaChecksums fromdisk = new VxSchemaChecksums();
            fromdisk.ReadChecksums(tmpdir);

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
            using (StreamWriter sw = File.AppendText(
                wv.PathCombine(tmpdir, "DATA", "Decoy")))
            {
                sw.WriteLine("Decoy file, shoudln't have checksums");
            }

            VxSchemaChecksums mangled = new VxSchemaChecksums();
            mangled.ReadChecksums(tmpdir);

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

    public static void Main()
    {
        WvTest.DoMain();
    }
}
