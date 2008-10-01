using System;

namespace Wv
{
    public class WvLoopback : WvStream
    {
	WvBuf mybuf = new WvBuf();
	
	public WvLoopback()
	{
	    post_writable();
	}
	
	public override int read(WvBytes b)
	{
	    int max = b.len < mybuf.used ? b.len : mybuf.used;
	    b.put(0, mybuf.get(max));
	    if (mybuf.used > 0)
		post_readable();
	    return max;
	}
	
	public override int write(WvBytes b)
	{
	    mybuf.put(b);
	    post_readable();
	    post_writable();
	    return b.len;
	}
    }
}
