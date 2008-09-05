using System;
using System.Linq;
using System.Data;
using System.Data.Common;
using System.IO;
using Wv.Mono.Terminal;
using Wv;
using Wv.Extensions;

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
	    
	    while (true)
	    {
		Console.WriteLine();
		inp = le.Edit("vx> ", "");
		if (inp == null) break;
		if (inp == "") continue;
		bool first = true;
		try
		{
		    foreach (var row in dbi.select(inp))
		    {
			if (first)
			{
			    var colnames =
				from c in row.columns
				select c.name.ToUpper();
			    Console.Write(wv.fmt("{0}\n\n",
						 colnames.Join(",")));
			    first = false;
			}
			Console.Write(wv.fmt("{0}\n", row.Join(",")));
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
