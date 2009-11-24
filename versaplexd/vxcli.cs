/*
 * Versaplex:
 *   Copyright (C)2007-2008 Versabanq Innovations Inc. and contributors.
 *       See the included file named LICENSE for license information.
 */
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using Wv.Mono.Terminal;
using Wv;
using Wv.Extensions;

namespace Wv
{
    public class VxDbException : DbException
    {
        public VxDbException(string msg) : base(msg)
	{
	}
    }
    
    [WvMoniker]
    public class WvDbi_Versaplex : WvDbi
    {
	WvDbus bus;
	
	struct ColInfo
	{
	    public int size;
	    public string name;
	    public string type;
	    public short precision;
	    public short scale;
	    public byte nullable;
	}
	
	public static void wvmoniker_register()
	{
	    WvMoniker<WvDbi>.register("vx",
		 (string m, object o) => new WvDbi_Versaplex(m));
	}
	
	public WvDbi_Versaplex(string busurl)
	{
	    WvUrl url = new WvUrl(busurl);
	    
	    if (url.host.e() || url.host == "session")
		bus = WvDbus.session_bus;
	    else
		bus = new WvDbus(wv.fmt("tcp:host={0},port={1}",
				    url.host, url.port));
	}
	
	public override WvSqlRows select(string sql, params object[] args)
	{
	    var call = new WvDbusCall("vx.versaplexd", "/db", "vx.db",
				      "ExecRecordset", "s");
	    WvDbusWriter writer = new WvDbusWriter();
	    writer.Write(sql);
	    call.write(writer);
	    
	    WvDbusMsg reply = bus.send_and_wait(call);
	    if (reply.err == "vx.db.sqlerror")
		throw new VxDbException(reply.iter().pop());
	    reply.check("a(issnny)vaay");
	    var it = reply.iter();
		    
	    // decode the raw column info
	    var l = new List<WvColInfo>();
	    foreach (IEnumerable<WvAutoCast> c in it.pop())
	    {
		int size;
		string name, type;
		short precision, scale;
		byte nullable;
		c.ToArray().assignto(out size, out name, out type,
				     out precision, out scale,
				     out nullable);
		
		l.Add(new WvColInfo(name, typeof(string),
				    nullable != 0,
				    size, precision, scale));
	    }
	    
	    WvColInfo[] colinfo = l.ToArray();
	    var rows = new List<WvSqlRow>();
	    foreach (var r in it.pop())
		rows.Add(new WvSqlRow(r.Cast<object>().ToArray(), 
				      colinfo));
	    
	    return new WvSqlRows_Versaplex(rows.ToArray(), colinfo);
	}
	
	public override int execute(string sql, params object[] args)
	{
	    using (select(sql, args))
		return 0;
	}
    }
    
    class WvSqlRows_Versaplex : WvSqlRows, IEnumerable<WvSqlRow>
    {
	WvSqlRow[] rows;
	WvColInfo[] schema;
	
	public WvSqlRows_Versaplex(WvSqlRow[] rows, WvColInfo[] schema)
	{
	    this.rows = rows;
	    this.schema = schema;
	}
	
	public override IEnumerable<WvColInfo> columns
	    { get { return schema; } }

	public override IEnumerator<WvSqlRow> GetEnumerator()
	{
	    foreach (var row in rows)
		yield return row;
	}
    }
}

public static class VxCli
{
    public static int Main(string[] args)
    {
	WvLog.maxlevel = WvLog.L.Debug;
	WvLog log = new WvLog("vxcli");

	if (args.Length != 1)
	{
	    Console.Error.WriteLine("Usage: vxcli <db-connection-string>");
	    return 1;
	}
	
	string moniker = args[0];
	
	WvIni vxini = new WvIni("versaplexd.ini");
	if (vxini.get("Connections", moniker) != null)
	    moniker = vxini.get("Connections", moniker);
	
	WvIni bookmarks = new WvIni(
		    wv.PathCombine(wv.getenv("HOME"), ".wvdbi.ini"));
	if (!moniker.Contains(":")
	    && bookmarks["Bookmarks"].ContainsKey(moniker))
	{
	    moniker = bookmarks["Bookmarks"][moniker];
	}
	else
	{
	    // not found in existing bookmarks, so see if we can parse and
	    // save instead.
	    WvUrl url = new WvUrl(moniker);
	    string path = url.path;
	    if (path.StartsWith("/"))
		path = path.Substring(1);
	    if (path != "" && url.host != null)
	    {
		log.print("Creating bookmark '{0}'\n", path);
		bookmarks.set("Bookmarks", path, moniker);
		try {
		    bookmarks.save();
		} catch (IOException) {
		    // not a big deal if we can't save our bookmarks.
		}
	    }
	}
	    
	using (var dbi = WvDbi.create(moniker))
	{
	    LineEditor le = new LineEditor("VxCli");
	    string inp;
	    
	    // Temporary solution to solve a bug when running with Mono JIT
	    // compiler version 2.0.1 for AMD64 architecture. When a clause that
	    // do not return any value is called first, the method throws a
	    // NullReferenceException
	    dbi.select("select 1").Dispose();
	    
	    while (true)
	    {
		Console.WriteLine();
		inp = le.Edit("vx> ", "");
		if (inp == null) break;
		if (inp == "") continue;
		try
		{
		    using (var result = dbi.select(inp))
		    {
			var colnames =
			    from c in result.columns
			    select c.name.ToUpper();
			Console.Write(wv.fmt("{0}\n\n",
					     colnames.join(",")));
			
			foreach (var row in result)
			    Console.Write(wv.fmt("{0}\n", row.join(",")));
		    }
		}
		catch (DbException e)
		{
		    Console.Write(wv.fmt("ERROR: {0}\n", e.Short()));
		}
	    }
	}
	
	return 0;
    }
}
