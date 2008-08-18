using System;
using Wv;
using Wv.Extensions;
using Wv.NDesk.Options;
using NDesk.DBus;

public static class Export
{
    static void DoExport(string bus_moniker, string exportdir)
    {
        VxDbusSchema dbus = new VxDbusSchema(bus_moniker);
        VxDiskSchema disk = new VxDiskSchema(exportdir);

        VxSchema.CopySchema(dbus, disk);
    }

    static void ShowHelp()
    {
        Console.Error.WriteLine("Usage: export [-b dbus-moniker] <outputdir>\n" + 
            "  Updates an SQL schema from Versaplex into the outputdir.\n" + 
            "  If dbus-moniker is not specified, uses DBUS_SESSION_BUS_ADDRESS.\n");
    }

    public static void Main(string[] args)
    {
        WvLog log = new WvLog("Export");

        string bus = null;
        string exportdir = null;
        var extra = new OptionSet()
            .Add("b=|bus=", delegate(string v) { bus = v; })
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

        DoExport(bus, exportdir);
    }
}
