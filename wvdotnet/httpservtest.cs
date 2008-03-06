using System;
using System.IO;
using System.Collections.Generic;
using Wv;
using Wv.HttpServer;

class HttpServTest
{
    static void do_request(WvHttpRequest req, Stream s)
    {
	WvLog log = new WvLog("do_request", WvLog.L.Info);
	log.print("Handling...");
	foreach (KeyValuePair<string,string> p in req.headers)
	    log.print("Header: '{0}' = '{1}'", p.Key, p.Value);
	
	using (StreamWriter w = new StreamWriter(s))
	{
	    w.WriteLine("HTTP/1.0 200 OK");
	    w.WriteLine("Content-type: text/plain");
	    w.WriteLine("");
	    w.WriteLine("Hello world!  Your path was '{0}'",
			req.path);
	    w.WriteLine("Query string: '{0}'", req.query_string);
	}
    }
    
    public static void Main()
    {
	WvHttpServer serv = new WvHttpServer(8001, do_request);

	while (true)
	    serv.runonce();
    }
}
