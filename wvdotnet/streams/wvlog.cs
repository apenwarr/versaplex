using System;
using System.IO;

namespace Wv
{
    public class WvLog: WvStream
    {
	Stream outstr = Console.OpenStandardError();
	WvBuf outbuf = new WvBuf();
	string name;

	public WvLog(string name)
	{
	    this.name = name;
	}

	public override int write(byte[] buf, int offset, int len)
	{
	    outbuf.put(buf, (uint)offset, (uint)len);
	    uint i;
	    while ((i = outbuf.strchr('\n')) > 0)
	    {
		byte[] b = (name + ": ").ToUTF8();
		outstr.Write(b, 0, b.Length);
		b = outbuf.get(i);
		outstr.Write(b, 0, b.Length);
	    }
	    return len;
	}
    }
}

