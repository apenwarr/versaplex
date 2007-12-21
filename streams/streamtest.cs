using System;
using System.Collections;

public static class wv
{
    public static void assert(bool b)
    {
	if (!b)
	    throw new Exception("assertion failure");
    }    
}

public class FooTest
{
    static IEnumerable contprint(WvLog log, WvStream s, string prefix)
    {
	int i = 0;
	while (s.isok)
	{
	    i++;
	    string str = s.read(128).FromUTF8();
	    log.print("{0}#{1}: {2}\n", prefix, i, str);
	    yield return 0;
	}
    }
    
    public static void Main()
    {
	{
	    WvLog log = new WvLog("main");
	    log.print("Hello");
	    log.print(" world!\n");
	    
	    WvEventer ev = new WvEventer();
	    WvStream s1 = new WvTcp(ev, "localhost");
	    WvStream s2 = new WvTcp(ev, "localhost");
	    s1.onreadable(contprint(log, s1, "\nA\n").ToAction());
	    s2.onreadable(contprint(log, s2, "\nB\n").ToAction());
	    s1.print("GET / HTTP/1.0\r\n\r\n");
	    s2.print("FOO / HTTP/1.0\r\n\r\n");
	    while (s1.isok || s2.isok)
		ev.run();
	    log.print("\n");
	}
    }
}
