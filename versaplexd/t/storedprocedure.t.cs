#include "wvtest.cs.h"
// Test the StoredProcedure class

using System;
using Wv;
using Wv.Test;
using Wv.Extensions;

[TestFixture]
class StoredProcedureTests
{
    delegate string StripComments(string s);

    [Test, Category("StoredProcedure")]
    public void TestStripComments()
    {
        // Use a delegate just so things fit on one line.
        var strip = new StripComments(StoredProcedure.StripComments);

        WVPASSEQ(strip("Hi"), "Hi");
        WVPASSEQ(strip("--foo"), "");
        WVPASSEQ(strip("--foo\nHi"), "\nHi");
        WVPASSEQ(strip("Hi\n--foo"), "Hi\n");
        WVPASSEQ(strip("Hi\n--foo\nThere"), "Hi\n\nThere");
        WVPASSEQ(strip("--foo"), "");
        WVPASSEQ(strip("/*foo*/"), "");
        WVPASSEQ(strip("/*foo\nbar*/"), "");
        WVPASSEQ(strip("Hi/*foo*/"), "Hi");
        WVPASSEQ(strip("Hi/*foo\nbar*/There"), "HiThere");
        WVPASSEQ(strip("Hi\n/* foo\nbar */\nThere"), "Hi\n\nThere");
        WVPASSEQ(strip("Hi--asdf/*\nThere/*foo*/"), "Hi\nThere");
        // Note: this is a bit fishy, but it's how the Perl version worked.
        WVPASSEQ(strip("Hi/*foo\n--asdf*/There"), "Hi/*foo\n");
    }

    [Test, Category("StoredProcedure")]
    public void TestBasicParsing()
    {
        string proc1 = "CREATE PROC mysp @arg int=1 AS select 1\n";
        var sp = new StoredProcedure(proc1);
        WVPASSEQ(sp.name, "mysp");
        var args = sp.args.ToArray();
        WVPASSEQ(args[0].name, "arg");
        WVPASSEQ(args[0].type, "int");
        WVPASSEQ(args[0].defval, "1");

        string proc2 = "CREATE PROC [mysp2]\n@arg1 int output ,\n" + 
            "  @arg2 varchar(10) = 'asdf',@arg3 money AS ";

        sp = new StoredProcedure(proc2);
        WVPASSEQ(sp.name, "[mysp2]");
        args = sp.args.ToArray();
        WVPASSEQ(args[0].name, "arg1");
        WVPASSEQ(args[0].type, "int");
        WVPASSEQ(args[0].defval, "");
        WVPASSEQ(args[1].name, "arg2");
        WVPASSEQ(args[1].type, "varchar(10)");
        WVPASSEQ(args[1].defval, "'asdf'");
        WVPASSEQ(args[2].name, "arg3");
        WVPASSEQ(args[2].type, "money");
        WVPASSEQ(args[2].defval, "");
    }

    public static void Main()
    {
        WvTest.DoMain();
    }
}
