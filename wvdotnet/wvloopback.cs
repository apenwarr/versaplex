using System;

namespace Wv
{
    public class WvLoopback : WvStream
    {
	WvBuf mybuf = new WvBuf();
	
	public override int read(byte[] buf, int offset, int len)
	{
	    int max = len < mybuf.used ? len : mybuf.used;
	    Array.Copy(mybuf.get(max), 0, buf, offset, len);
	    return max;
	}
	
	public override int write(byte[] buf, int offset, int len)
	{
	    mybuf.put(buf, offset, len);
	    do_readable();
	}
    }
}
