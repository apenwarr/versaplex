#include "wvtest.cs.h"

using System;
using System.Linq;
using Wv;
using Wv.Test;

[TestFixture]
class DbusTest
{
    [Test]
    public void create_bus()
    {
        Bus bus = new Bus(Address.Session);
	WVPASS(true);
    }
    
    [Test] void message_read_write()
    {
	
    }

    public static void Main()
    {
	WvTest.DoMain();
    }
}
