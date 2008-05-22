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

    [Test, Category("Schemamatic"), Category("GetSchemaChecksums")]
    public void TestProcedureChecksums()
    {
        try { VxExec("drop procedure Func1"); } catch { }
        try { VxExec("drop procedure Func2"); } catch { }

        VxSchemaChecksums sums;
// FIXME: Either dbus-sharp or wvdbusd doesn't properly send back empty
// replies.  See Versaplex Google Code bug 37.
#if 0
        sums = VxGetSchemaChecksums();
        WVPASSEQ(sums.Count, 0);
#endif

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

        WVASSERT(VxExec("create procedure Func2 as select '" + msg2 + "'"));

        WVASSERT(VxScalar("exec Func2", out outmsg));
        WVPASSEQ(msg2, (string)outmsg);

        sums = VxGetSchemaChecksums();

        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVASSERT(sums.ContainsKey("Procedure/Func2"));
        WVPASSEQ(sums["Procedure/Func1"].checksums.Count, 1);
        WVPASSEQ(sums["Procedure/Func2"].checksums.Count, 1);
        WVPASSEQ(sums["Procedure/Func1"].checksums[0], 0x55F9D9E3);
        WVPASSEQ(sums["Procedure/Func2"].checksums[0], 0x25AA9C37);

        try { VxExec("drop procedure Func2"); } catch { }

        sums = VxGetSchemaChecksums();

        WVASSERT(sums.ContainsKey("Procedure/Func1"));
        WVFAIL(sums.ContainsKey("Procedure/Func2"));
        WVPASSEQ(sums["Procedure/Func1"].checksums.Count, 1);
        WVPASSEQ(sums["Procedure/Func1"].checksums[0], 0x55F9D9E3);
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
    }

    public static void Main()
    {
        WvTest.DoMain();
    }
}
