#include "wvtest.cs.h"
// Test the Schemamatic functions that live in the Versaplex daemon.

using System;
using System.Collections.Generic;
using Wv;
using Wv.Test;
using NDesk.DBus;

[TestFixture]
class SchemamaticTests : VersaplexTester
{
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
        {
            object errname;
            if (!reply.Header.Fields.TryGetValue(FieldCode.ErrorName,
                        out errname))
                throw new Exception("D-Bus error received but no error name "
                        +"given");

            object errsig;
            if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out errsig) || errsig.ToString() != "s")
                throw new DbusError(errname.ToString());

            MessageReader mr = new MessageReader(reply);

            object errmsg;
            mr.GetValue(typeof(string), out errmsg);

            throw new DbusError(errname.ToString() + ": " + errmsg.ToString());
        }
        default:
            throw new Exception("D-Bus response was not a method return or "
                    +"error");
        }
    }

    VxSchema VxGetSchema()
    {
	Console.WriteLine(" + VxGetSchema");

        Message call = CreateMethodCall("GetSchema", "");

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
        {
            object errname;
            if (!reply.Header.Fields.TryGetValue(FieldCode.ErrorName,
                        out errname))
                throw new Exception("D-Bus error received but no error name "
                        +"given");

            object errsig;
            if (!reply.Header.Fields.TryGetValue(FieldCode.Signature,
                        out errsig) || errsig.ToString() != "s")
                throw new DbusError(errname.ToString());

            MessageReader mr = new MessageReader(reply);

            object errmsg;
            mr.GetValue(typeof(string), out errmsg);

            throw new DbusError(errname.ToString() + ": " + errmsg.ToString());
        }
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
        WVPASSEQ(sums.Count, 0);

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
        WVPASSEQ(sums.Count, 2);

        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVASSERT(sums.ContainsKey("Procedure-Encrypted/Func2"));
        WVPASSEQ(sums["Procedure/Func1"].checksums.Count, 1);
        WVPASSEQ(sums["Procedure-Encrypted/Func2"].checksums.Count, 1);
        WVPASSEQ(sums["Procedure/Func1"].checksums[0], 0x55F9D9E3);
        WVPASSEQ(sums["Procedure-Encrypted/Func2"].checksums[0], 0x458D4283);

        WVASSERT(VxExec("drop procedure Func2"));

        sums = VxGetSchemaChecksums();
        WVPASSEQ(sums.Count, 1);

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
        string query = "CREATE TABLE [Tab1] (" + 
            "[f1] [int]  NOT NULL IDENTITY(1, 1)," +
            "[f2] [money]  NULL," + 
            "[f3] [varchar] (80) NULL)";
        WVASSERT(VxExec(query));

        VxSchemaChecksums sums;
        sums = VxGetSchemaChecksums();

        // Three columns gives us three checksums
        WVPASSEQ(sums["Table/Tab1"].checksums.Count, 3);
        WVPASSEQ(sums["Table/Tab1"].checksums[0], 0x588AEDDC)
        WVPASSEQ(sums["Table/Tab1"].checksums[1], 0x065BBC3B)
        WVPASSEQ(sums["Table/Tab1"].checksums[2], 0x279DBF24)

        WVASSERT(VxExec("drop table Tab1"));
    }

    [Test, Category("Schemamatic"), Category("GetSchemaChecksums")]
    public void TestIndexChecksums()
    {
        try { VxExec("drop table Tab1"); } catch { }
        string query = "CREATE TABLE [Tab1] (" + 
            "[f1] [int]  NOT NULL IDENTITY(1, 1)," +
            "[f2] [money]  NULL," + 
            "[f3] [varchar] (80) NULL)";
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

    [Test, Category("Schemamatic"), Category("GetSchemaChecksums")]
    public void TestXmlSchemaChecksums()
    {
        try { VxExec("drop xml schema collection TestSchema"); } catch { }
        // To escape a double-quote within an @" string, use "".  This
        // looks a bit hideous, but better than a normal string (esp printed).
        string query = @"CREATE XML SCHEMA COLLECTION TestSchema AS
            N'<xsd:schema xmlns:xsd=""http://www.w3.org/2001/XMLSchema"">
            <xsd:element name=""Employee"">
                <xsd:complexType>
                    <xsd:sequence>
                        <xsd:element name=""SIN"" type=""xsd:string""/>
                        <xsd:element name=""Name"" type=""xsd:string""/>
                        <xsd:element name=""DateOfBirth"" type=""xsd:date""/>
                        <xsd:element name=""EmployeeType"" type=""xsd:string""/>
                        <xsd:element name=""Salary"" type=""xsd:long""/>
                    </xsd:sequence>
                </xsd:complexType>
            </xsd:element>
          </xsd:schema>'";
        WVASSERT(VxExec(query));

        VxSchemaChecksums sums;
        sums = VxGetSchemaChecksums();

        WVPASSEQ(sums["XMLSchema/TestSchema"].checksums.Count, 1);
        WVPASSEQ(sums["XMLSchema/TestSchema"].checksums[0], 0xFA7736B3);

        WVASSERT(VxExec("drop xml schema collection TestSchema"));
    }

    [Test, Category("Schemamatic"), Category("GetSchema")]
    public void TestGetProcSchema()
    {
        try { VxExec("drop procedure Func1"); } catch { }
        try { VxExec("drop procedure Func2"); } catch { }

        string msg1 = "Hello, world, this is Func1!";
        string msg2 = "Hello, world, this is Func2!";
        string fmt = "create procedure {0} {1} as select '{2}'";
        string query1 = String.Format(fmt, "Func1", "", msg1);
        string query2 = String.Format(fmt, "Func2", "with encryption", msg2);
        object outmsg;
        WVASSERT(VxExec(query1));
        WVASSERT(VxScalar("exec Func1", out outmsg));
        WVPASSEQ(msg1, (string)outmsg);

        WVASSERT(VxExec(query2));
        WVASSERT(VxScalar("exec Func2", out outmsg));
        WVPASSEQ(msg2, (string)outmsg);

        VxSchema schema = VxGetSchema();

        WVASSERT(schema.ContainsKey("Procedure/Func1"));
        WVPASSEQ(schema["Procedure/Func1"].name, "Func1");
        WVPASSEQ(schema["Procedure/Func1"].type, "Procedure");
        WVPASSEQ(schema["Procedure/Func1"].encrypted, false);
        WVPASSEQ(schema["Procedure/Func1"].text, query1);

        WVASSERT(schema.ContainsKey("Procedure-Encrypted/Func2"));
        WVPASSEQ(schema["Procedure-Encrypted/Func2"].name, "Func2");
        WVPASSEQ(schema["Procedure-Encrypted/Func2"].type, "Procedure");
        WVPASSEQ(schema["Procedure-Encrypted/Func2"].encrypted, true);
        // FIXME: Can't yet retrieve the contents of encrypted functions
        //WVPASSEQ(schema["Procedure-Encrypted/Func2"].text, query2);

        WVASSERT(VxExec("drop procedure Func1"));
        WVASSERT(VxExec("drop procedure Func2"));
    }

    [Test, Category("Schemamatic"), Category("GetSchema")]
    public void TestGetIndexSchema()
    {
        try { VxExec("drop table Tab1"); } catch { }
        string query = "CREATE TABLE [Tab1] (" + 
            "[f1] [int]  NOT NULL PRIMARY KEY," +
            "[f2] [money]  NULL," + 
            "[f3] [varchar] (80) NULL)";
        WVASSERT(VxExec(query));

	string idx1q = "CREATE UNIQUE INDEX [Idx1] ON [Tab1] \n" + 
	    "\t(f2,f3 DESC);\n\n";
        WVASSERT(VxExec(idx1q));

	VxSchema schema = VxGetSchema();

	WVPASSEQ(schema.Count, 2);

	WVASSERT(schema.ContainsKey("Index/Idx1"));
	WVPASSEQ(schema["Index/Idx1"].name, "Idx1");
	WVPASSEQ(schema["Index/Idx1"].type, "Index");
	WVPASSEQ(schema["Index/Idx1"].encrypted, false);
	WVPASSEQ(schema["Index/Idx1"].text.Length, idx1q.Length);
	WVPASSEQ(schema["Index/Idx1"].text, idx1q);

	string pk_name = "";
	bool found_pk = false;

	// Primary key names are generated and unpredictable.  Just make sure
	// that we only got one back, and that it looks like the right one.
	foreach (string key in schema.Keys)
	    if (key.StartsWith("Index/PK__Tab1"))
	    {
		WVASSERT(!found_pk);
		found_pk = true;
		pk_name = key.Substring("Index/".Length);
		Console.WriteLine("Found primary key index " + pk_name);
		// Note: don't break here, so we can check there aren't others.
	    }
	WVASSERT(found_pk);

	WVASSERT(schema.ContainsKey("Index/" + pk_name));
	WVPASSEQ(schema["Index/" + pk_name].name, pk_name);
	WVPASSEQ(schema["Index/" + pk_name].type, "Index");
	WVPASSEQ(schema["Index/" + pk_name].encrypted, false);
	string pk_query = String.Format(
	    "ALTER TABLE [Tab1] ADD CONSTRAINT [{0}] PRIMARY KEY CLUSTERED\n" +
	    "\t(f1);\n\n", pk_name);
	WVPASSEQ(schema["Index/" + pk_name].text.Length, pk_query.Length);
	WVPASSEQ(schema["Index/" + pk_name].text, pk_query);

        WVASSERT(VxExec("drop index Tab1.Idx1"));
        WVASSERT(VxExec("drop table Tab1"));
    }

    [Test, Category("Schemamatic"), Category("GetSchema")]
    public void TestGetXmlSchemas()
    {
        try { VxExec("drop xml schema collection TestSchema"); } catch { }
        try { VxExec("drop xml schema collection TestSchema2"); } catch { }

        string query1 = "CREATE XML SCHEMA COLLECTION [dbo].[TestSchema] AS " + 
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

	VxSchema schema = VxGetSchema();

        WVPASSEQ(schema.Count, 2);

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

    public static void Main()
    {
        WvTest.DoMain();
    }
}
