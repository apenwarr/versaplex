using System;
using System.IO;

namespace Wv
{
    public class WvLog: WvStream
    {
	Stream outstr = Console.OpenStandardError();
	WvBuf outbuf = new WvBuf();
	//string name;
	byte[] header;

	public WvLog(string name)
	{
	    //this.name = name;
	    header = String.Format("<{0}> ", name).ToUTF8();
	}

	public override int write(byte[] buf, int offset, int len)
	{
	    outbuf.put(buf, (uint)offset, (uint)len);
	    uint i;
	    while ((i = outbuf.strchr('\n')) > 0)
	    {
		outstr.Write(header, 0, header.Length);
		byte[] b = outbuf.get(i);
		outstr.Write(b, 0, b.Length);
	    }
	    return len;
	}
    }
}

