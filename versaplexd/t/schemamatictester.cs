#include "wvtest.cs.h"
// A VersaplexTester that has some utility functions useful for Schemamatic.

using System;
using System.Collections.Generic;
using Wv;
using Wv.Test;

class SchemamaticTester : VersaplexTester
{
    // Utility class to create some test schema for the Schemamatic tests.
    public class SchemaCreator
    {
        public string tab1q;
        public string tab1q_nopk;
        public string tab2q;
        public string idx1q;
        public string msg1;
        public string msg2;
        public string func1q;
        public string func2q;
        public string xmlq;

        VersaplexTester t;

        public SchemaCreator(VersaplexTester _t)
        {
            t = _t;

            Cleanup();

            tab1q = "CREATE TABLE [Tab1] (\n" + 
                "\t[f1] [int]  NOT NULL PRIMARY KEY,\n" +
                "\t[f2] [money]  NULL,\n" + 
                "\t[f3] [varchar] (80) NULL);\n\n";
            tab1q_nopk = "CREATE TABLE [Tab1] (\n" + 
                "\t[f1] [int]  NOT NULL,\n" +
                "\t[f2] [money]  NULL,\n" + 
                "\t[f3] [varchar] (80) NULL);\n\n";
            tab2q = "CREATE TABLE [Tab2] (\n" + 
                "\t[f4] [binary] (1) NOT NULL);\n\n";
            idx1q = "CREATE UNIQUE INDEX [Idx1] ON [Tab1] \n" + 
                "\t(f2, f3 DESC);\n\n";
            msg1 = "Hello, world, this is Func1!";
            msg2 = "Hello, world, this is Func2!";
            func1q = "create procedure Func1 as select '" + msg1 + "'\n";
            func2q = "create function Func2 () returns varchar as begin " + 
                "return '" + msg2 + "'; end\n";
            xmlq = "\nCREATE XML SCHEMA COLLECTION [dbo].[TestSchema] AS " + 
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

        public void Create()
        {
            WVASSERT(t.VxExec(tab1q));
            WVASSERT(t.VxExec(tab2q));
            WVASSERT(t.VxExec(idx1q));
            WVASSERT(t.VxExec(func1q));
            WVASSERT(t.VxExec(func2q));
            WVASSERT(t.VxExec(xmlq));
        }

        public void Cleanup()
        {
            try { t.VxExec("drop index Tab1.Idx1"); } catch { }
            try { t.VxExec("drop table Tab1"); } catch { }
            try { t.VxExec("drop table Tab2"); } catch { }
            try { t.VxExec("drop xml schema collection TestSchema"); } catch { }
            try { t.VxExec("drop procedure Func1"); } catch { }
            try { t.VxExec("drop function Func2"); } catch { }
        }
    }

    public void TestSchemaEquality(VxSchema left, VxSchema right)
    {
        foreach (KeyValuePair<string,VxSchemaElement> p in right)
        {
            WVPASSEQ(left[p.Key].type, p.Value.type);
            WVPASSEQ(left[p.Key].name, p.Value.name);
            WVPASSEQ(left[p.Key].text, p.Value.text);
            WVPASSEQ(left[p.Key].encrypted, p.Value.encrypted);
        }
        WVPASSEQ(left.Count, right.Count);
    }

    public void TestChecksumEquality(VxSchemaChecksums left, 
        VxSchemaChecksums right)
    {
        WVPASSEQ(left.Count, right.Count);
        foreach (KeyValuePair<string,VxSchemaChecksum> p in right)
        {
            WVPASSEQ(left[p.Key].GetSumString(), p.Value.GetSumString());
        }
    }

    // Checks that the schema contains a primary key for tablename, and
    // returns the primary key's name.
    public string CheckForPrimaryKey(VxSchema schema, string tablename)
    {
        WvLog log = new WvLog("CheckForPrimaryKey");
	string pk_name = null;

        string prefix = "Index/" + tablename + "/";

	// Primary key names are generated and unpredictable.  Just make sure
	// that we only got one back, and that it looks like the right one.
	foreach (string key in schema.Keys)
        {
            log.print("Looking at " + key);
	    if (key.StartsWith(prefix + "PK__" + tablename))
	    {
		WVASSERT(pk_name == null)
		pk_name = key.Substring(prefix.Length);
		log.print("Found primary key index " + pk_name);
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

}
