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
        new Bus(Address.Session);
	WVPASS(true);
    }
    
    [Test]
    public void message_read_write()
    {
	Message m = new Message();
	m.Signature = new Signature("si");
    }

    public static void Main()
    {
	WvTest.DoMain();
    }
}
