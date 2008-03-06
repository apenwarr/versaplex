using System;
using System.IO;
using System.Net.Sockets;
using System.Collections.Generic;
using Wv;

namespace Wv.HttpServer
{
    public class WvHttpRequest
    {
	public string request_uri;

	public string path
	{
	    get { return request_uri.Split(new char[] {'?'}, 2)[0]; }
	}

	public string query_string
	{
	    get
	    {
		string[] parts = request_uri.Split(new char[] {'?'}, 2);
		if (parts.Length >= 2)
		    return parts[1];
		else
		    return "";
	    }
	}

	public Dictionary<string,string> headers
	    = new Dictionary<string,string>();

	public WvHttpRequest() { }

	public void parse_request(string s)
	{
	    string[] parts = s.Split(new char[] {' '}, 3);
	    if (parts.Length < 3)
		throw new Exception("Not enough words in request!");
	    if (parts[0] != "GET")
		throw new Exception("Request should start with GET");
	    request_uri = parts[1];
	}

	public void parse_header(string s)
	{
	    if (s == "") return;

	    string[] parts = s.Split(new char[] {':'}, 2,
				     StringSplitOptions.None);
	    headers.Remove(parts[0]);
	    if (parts.Length < 2)
		headers.Add(parts[0], "");
	    else
		headers.Add(parts[0], parts[1].Trim());
	}
    }

    public class WvHttpServer
    {
	TcpListener server;
	WvLog log = new WvLog("HTTP Server", WvLog.L.Info);

	public delegate void Responder(WvHttpRequest req, Stream s);
	Responder responder;

	public WvHttpServer(int port, Responder responder)
	{
	    this.responder = responder;
	    log.print("World's dumbest http server initializing.");
	    log.print("Listening on port {0}.", port);
	    server = new TcpListener(port);
	    server.Start();
	}

	public void runonce()
	{
	    TcpClient client = server.AcceptTcpClient();
	    log.print("Incoming connection.");

	    NetworkStream stream = client.GetStream();
	    StreamReader r = new StreamReader(stream);

	    WvHttpRequest req = new WvHttpRequest();
	    string s = r.ReadLine();
	    log.print("Got request line: '{0}'", s);
	    req.parse_request(s);
	    log.print("Path is: '{0}'", req.request_uri);
	    do
	    {
		s = r.ReadLine();
		log.print("Got header line: '{0}'", s);
		req.parse_header(s);
	    }
	    while (s != "");

	    responder(req, stream);

	    log.print("Closing connection.");
	    client.Close();
	}
    }
}
