using System;
using System.Text;
using System.Collections.Generic;
using System.IO;
using Wv;
using Wv.Extensions;
using Wv.NDesk.Options;
using NDesk.DBus;

public static class ApplyData
{
    static WvLog log = new WvLog("ApplyData");
    static WvLog err = new WvLog("ApplyData", WvLog.L.Error);

    // Parses a 5-digit positive number from pri.  Returns -1 on error.
    static int ParsePriority(string pri, bool verbose)
    {
        int charcount = 0;
        foreach (char c in pri)
        {
            charcount++;
            if (!Char.IsDigit(c))
            {
                charcount = -1;
                break;
            }
        }
        if (charcount != 5 || pri.Length != 5)
        {
            if (verbose)
                err.print("Priority code '{0}' must be a 5-digit number.\n", 
                        pri);
            return -1;
        }

        return Int32.Parse(pri);
    }

    // Extracts the table name and priority out of a path.  E.g. for 
    // "/foo/12345-bar.sql", returns "12345" and "bar" as the priority and 
    // table name.  Returns -1/null if the parse fails.
    static void ParsePath(string pathname, out int seqnum, 
        out string tablename)
    {
        FileInfo info = new FileInfo(pathname);
        seqnum = -1;
        tablename = null;

        int dashidx = info.Name.IndexOf('-');
        if (dashidx < 0)
            return;

        string pristr = info.Name.Remove(dashidx);
        string rest = info.Name.Substring(dashidx + 1);
        int pri = ParsePriority(pristr, false);

        if (pri < 0)
            return;

        if (info.Extension.ToLower() == ".sql")
            rest = rest.Remove(rest.ToLower().LastIndexOf(".sql"));
        else
            return;

        seqnum = pri;
        tablename = rest;

        return;
    }

    static void ApplyFiles(string[] files, VxDbusSchema dbus, bool dry_run)
    {
        int seqnum;
        string tablename;

        Array.Sort(files);
        foreach (string file in files)
        {
            string data = File.ReadAllText(file, System.Text.Encoding.UTF8);
            ParsePath(file, out seqnum, out tablename);

            if (tablename == "ZAP")
                tablename = "";

            log.print("Applying data from {0}\n", file);
            if (dry_run)
                continue;

            if (seqnum > 0)
            {
                dbus.PutSchemaData(tablename, data, seqnum);
            }
            else
            {
                // File we didn't generate, try to apply it anyway.
                dbus.PutSchemaData("", data, 0);
            }
        }
    }

    static void DoApplyData(string bus_moniker, string exportdir, bool dry_run)
    {
        if (!Directory.Exists(exportdir))
            return;

        VxDbusSchema dbus = new VxDbusSchema(bus_moniker);

        string datadir = Path.Combine(exportdir, "DATA");
        if (Directory.Exists(datadir))
            ApplyFiles(Directory.GetFiles(datadir), dbus, dry_run);
        else
            ApplyFiles(Directory.GetFiles(exportdir), dbus, dry_run);
    }

    static void ShowHelp()
    {
        Console.Error.WriteLine(
            @"Usage: applydata [-b dbus-moniker] [--dry-run] <outputdir>
  Updates an SQL schema from Versaplex into the outputdir.

  -b: specifies the dbus moniker to connect to.  If not provided, uses
      DBUS_SESSION_BUS_ADDRESS.
  --dry-run: lists the schema that would be changed but doesn't modify anything.
");
    }

    public static void Main(string[] args)
    {
        string bus = null;
        string exportdir = null;
        bool dry_run = false;

        var extra = new OptionSet()
            .Add("b=|bus=", delegate(string v) { bus = v; } )
            .Add("dry-run", delegate(string v) { dry_run = (v != null); } )
            .Parse(args);

        if (extra.Count != 1)
        {
            ShowHelp();
            return;
        }

        exportdir = extra[0];

        if (bus == null)
            bus = Address.Session;

        if (bus == null)
        {
            log.print
                ("DBUS_SESSION_BUS_ADDRESS not set and no -b option given.\n");
            ShowHelp();
        }

        log.print("Exporting to '{0}'\n", exportdir);
        log.print("Connecting to '{0}'\n", bus);

        DoApplyData(bus, exportdir, dry_run);
    }
}
