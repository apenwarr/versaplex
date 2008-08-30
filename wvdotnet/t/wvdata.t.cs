#include "wvtest.cs.h"
using System;
using Wv;
using Wv.Test;

[TestFixture]
public class WvDataTests
{
    [Test] public void nulls_test()
    {
	WvAutoCast n = WvAutoCast._null;
	object o = n;
	WVPASS(o != null);
	WVPASS(o.ToString() != null);
	WVPASSEQ((int)n, 0);
	WVPASSEQ((long)n, 0);
	WVPASSEQ((double)n, 0.0);
	
	n = new WvAutoCast("-6");
	o = n;
	WVPASS(o != null);
	WVPASSEQ(o.ToString(), "-6");
	WVPASSEQ((int)n, -6);
	WVPASSEQ((long)n, -6);
	WVPASSEQ((int)(((double)n)*10000), -60000);

	n = new WvAutoCast("-5.5555.p");
	o = n;
	WVPASS(o != null);
	WVPASSEQ(o.ToString(), "-5.5555.p");
	WVPASSEQ((int)n, -5);
	WVPASSEQ((long)n, -5);
	WVPASSEQ((int)(((double)n)*10000), -55555);
    }

    [Test]
    public void bool_test()
    {
        WvAutoCast t = new WvAutoCast(true);
        WvAutoCast f = new WvAutoCast(false);
        WVPASSEQ(t.ToString(), "1");
        WVPASSEQ(f.ToString(), "0");
        WVPASSEQ((double)t, 1.0);
        WVPASSEQ((double)f, 0.0);
        WVPASSEQ((int)t, 1);
        WVPASSEQ((int)f, 0);
    }
}
