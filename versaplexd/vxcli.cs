using System;
using System.Linq;
using Wv.Mono.Terminal;
using Wv;
using Wv.Extensions;

public static class VxCli
{
    public static int Main(string[] args)
    {
	WvLog log = new WvLog("vxcli");
	
	if (args.Length != 1)
	{
	    Console.Error.WriteLine("Usage: vxcli <db-connection-string>");
	    return 1;
	}
	
	string dburl = args[0];
	
	WvIni ini = new WvIni("versaplexd.ini");
	if (ini["Connections"][dburl] != null)
	    dburl = ini["Connections"][dburl];
	
	using (var dbi = new WvDbi(dburl))
	{
	    LineEditor le = new LineEditor("VxCli");
	    string inp;
	    while ((inp = le.Edit("vx> ", "")) != null)
	    {
		if (inp == "") continue;
		log.print("You typed: [{0}]\n", inp);
		foreach (var row in dbi.select(inp))
		{
		    var cols =
			from col in row
			select col.ToString();
		    log.print("{0}\n", cols.Join(","));
		}
	    }
	}
	
	return 0;
    }
}
