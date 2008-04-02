#include "wvtest.cs.h"

using System;
using Wv.Test;
using Wv;

[TestFixture]
public class WvStreamTests
{
    [Test] public void basics()
    {
	WvStream s = new WvStream();
	WVPASS(s.isok);
	s.close();
	WVFAIL(s.isok);
	
	int closed_called = 0, closed_called2 = 0;
	s = new WvStream();
	s.onclose += delegate() { closed_called++; };
	s.onclose += delegate() { closed_called2++; };
	Exception e1 = new Exception("e1");
	Exception e2 = new Exception("e2");
	WVPASS(s.isok);
	WVPASSEQ(closed_called, 0);
	WVPASSEQ(closed_called2, 0);
	s.err = e1;
	s.err = e2;
	WVFAIL(s.isok);
	WVPASS(s.err != null);
	WVPASS(s.err == e1);
	WVPASS(s.err.Message == "e1");
	WVPASSEQ(closed_called, 1);
	WVPASSEQ(closed_called2, 1);
    }
}
