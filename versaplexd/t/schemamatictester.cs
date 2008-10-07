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
        public string tab1sch;
        public string tab2q;
        public string tab2sch;
        public string msg1;
        public string msg2;
        public string func1q;
        public string func2q;
        public string xmlq;
        public string tabfuncq;
        public string triggerq;
        public string viewq;

        VersaplexTester t;

        public SchemaCreator(VersaplexTester _t)
        {
            t = _t;

            Cleanup();

            tab1q = "CREATE TABLE [Tab1] (\n" + 
                "\t[f1] [int]  NOT NULL,\n" +
                "\t[f2] [money]  NOT NULL,\n" + 
                "\t[f3] [varchar] (80) NULL);\n" + 
                "ALTER TABLE [Tab1] ADD CONSTRAINT [PK_Tab1] PRIMARY KEY (f1,f2)\n" +
                "CREATE UNIQUE INDEX [Idx1] ON [Tab1]\n" + 
                "\t(f2, f3 DESC);\n\n";
            tab1sch = "column: name=f1,type=int,null=0\n" + 
                "column: name=f2,type=money,null=0\n" + 
                "column: name=f3,type=varchar,null=1,length=80\n" +
                "index: column=f2,column=f3 DESC,name=Idx1,unique=1,clustered=2\n" +
                "primary-key: column=f1,column=f2,clustered=1\n";
            tab2q = "CREATE TABLE [Tab2] (\n" + 
                "\t[f4] [binary] (1) NOT NULL);\n\n";
            tab2sch = "column: name=f4,type=binary,null=0,length=1\n";
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
            tabfuncq = "create function TabFunc1 ( ) returns table as " + 
                "return (select 1 as col)\n";
            triggerq = "create trigger Trigger1 on Tab1 for insert as " + 
                "select 1\n";
            viewq = "create view View1 as select 1 as col\n";
        }

        public void Create()
        {
            WVASSERT(t.VxExec(tab1q));
            WVASSERT(t.VxExec(tab2q));
            WVASSERT(t.VxExec(func1q));
            WVASSERT(t.VxExec(func2q));
            WVASSERT(t.VxExec(xmlq));
            WVASSERT(t.VxExec(tabfuncq));
            WVASSERT(t.VxExec(triggerq));
            WVASSERT(t.VxExec(viewq));
        }

        public void Cleanup()
        {
            try { t.VxExec("drop view View1"); } catch { }
            try { t.VxExec("drop trigger Trig1"); } catch { }
            try { t.VxExec("drop table Tab1"); } catch { }
            try { t.VxExec("drop table Tab2"); } catch { }
            try { t.VxExec("drop xml schema collection TestSchema"); } catch { }
            try { t.VxExec("drop procedure Func1"); } catch { }
            try { t.VxExec("drop function Func2"); } catch { }
            try { t.VxExec("drop function TabFunc1"); } catch { }
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
}
