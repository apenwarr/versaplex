#include "wvtest.cs.h"

using System;
using System.Linq;
using Wv;
using Wv.Test;

[TestFixture]
class DbusTest
{
    [Test]
    public void basics()
    {
	WVPASS(true);
    }

    public static void Main()
    {
	WvTest.DoMain();
    }
}
