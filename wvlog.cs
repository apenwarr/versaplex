using System;
using System.IO;
using System.Threading;
using Wv.Extensions;

namespace Wv
{
    public class WvLogConsole
    {
	Stream outstr = Console.OpenStandardError();
	string open_header = null;
	byte[] nl = "\n".ToUTF8();
	
	void w(byte[] b)
	{
	    outstr.Write(b, 0, b.Length);
	}
	
	void w(string s)
	{
	    w(s.ToUTF8());
	}
	
	public void writelines(string header, WvBuf outbuf)
	{
	    // finish previous partial line, if necessary
	    if (outbuf.used > 0 
		&& open_header != null && open_header != header)
	    {
		w(nl);
		open_header = null;
	    }
	    
	    uint i;
	    while ((i = outbuf.strchr('\n')) > 0)
	    {
		if (open_header == null)
		    w(header);
		w(outbuf.get(i));
		open_header = null;
	    }
	    
	    // ending partial line
	    if (outbuf.used > 0)
	    {
		if (open_header == null)
		    w(header);
		w(outbuf.get(outbuf.used));
		open_header = header;
	    }
	}
    }
    
    public class WvLog : WvStream
    {
	public enum L {
	    Critical = 0,
	    Error,
	    Warning,
	    Notice,
	    Info,
	    Debug, Debug1=Debug,
	    Debug2,
	    Debug3,
	    Debug4,
	    Debug5,
	};
	
	static L _maxlevel = L.Info;
	public static L maxlevel { 
	    get { return _maxlevel; }
	    set { _maxlevel = value; }
	}
	static WvLogConsole recv = new WvLogConsole();
	
	string name;
	L level;
	
	string levelstr(L level)
	{
	    switch (level)
	    {
	    case L.Critical: return "!";
	    case L.Error:    return "E";
	    case L.Warning:  return "W";
	    case L.Notice:   return "N";
	    case L.Info:     return "I";
	    case L.Debug1:   return "1";
	    case L.Debug2:   return "2";
	    case L.Debug3:   return "3";
	    case L.Debug4:   return "4";
	    case L.Debug5:   return "5";
	    default:
		wv.assert(false, "Unknown loglevel??"); 
		return "??";
	    }
	}
	
	public WvLog(string name, L level)
	{
	    this.name = name;
	    this.level = level;
	}
	
	public WvLog(string name)
	    : this(name, L.Info)
	    { }
	
	public override int write(byte[] buf, int offset, int len)
	{
	    if (level > maxlevel)
		return len; // pretend it's written
	    
	    WvBuf outbuf = new WvBuf();
	    outbuf.put(buf, (uint)offset, (uint)len);
	    recv.writelines(wv.fmt("{0}<{1}>: ", name, levelstr(level)),
			    outbuf);
	    return len;
	}
	
	public void print(L level, object s)
	{
	    L old = this.level;
	    this.level = level;
	    print(s);
	    this.level = old;
	}
	
	public void print(L level, string fmt, params object[] args)
	{
	    print(level, (object)String.Format(fmt, args));
	}
	
	public override void print(object o)
	{
	    if (level > maxlevel)
		return;
	    base.print(o);
	}

	public WvLog split(L level)
	{
	    return new WvLog(name, level);
	}
    }
}

