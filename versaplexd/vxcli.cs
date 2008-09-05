using System;
using System.Linq;
using System.Data;
using System.Data.Common;
using Wv.Mono.Terminal;
using Wv;
using Wv.Extensions;

public static class VxCli
{
    public static int Main(string[] args)
    {
	WvLog.maxlevel = WvLog.L.Debug;

	if (args.Length != 1)
	{
	    Console.Error.WriteLine("Usage: vxcli <db-connection-string>");
	    return 1;
	}
	
	string dburl = args[0];
	
	WvIni vxini = new WvIni("versaplexd.ini");
	if (vxini.get("Connections", dburl) != null)
	    dburl = vxini.get("Connections", dburl);
	
	using (var dbi = WvDbi.create(dburl))
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
