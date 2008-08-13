#include "wvtest.cs.h"

using System;
using Wv.Test;
using NDesk.DBus;
using org.freedesktop.DBus;

[TestFixture]
public class MyClass
{
    [Test]
    public void hello()
    {
        WVPASS(true);
	Bus bus = Bus.Session;
	IBus ib = bus.GetObject<IBus>("org.freedesktop.DBus", new ObjectPath("/"));
	string[] names = ib.ListNames();
	Console.WriteLine("Names:");
	foreach (string s in names)
	    Console.WriteLine(s);
	WVPASS(names.Length > 0);
    }
    
    public static void Main()
    {
        WvTest.DoMain();
    }
}
