using System;
using Wv;
using Wv.Extensions;
using Wv.NDesk.Options;
using NDesk.DBus;

public static class Export
{
    static void DoExport(string bus_moniker, string exportdir, bool dry_run)
    {
        VxDbusSchema dbus = new VxDbusSchema(bus_moniker);
        VxDiskSchema disk = new VxDiskSchema(exportdir);

        VxCopyOpts opts = VxCopyOpts.Verbose;
        if (dry_run)
            opts |= VxCopyOpts.DryRun;

        VxSchemaErrors errs = VxSchema.CopySchema(dbus, disk, opts);

        foreach (var p in errs)
        {
            VxSchemaError err = p.Value;
            Console.WriteLine("Error applying {0}: {1} ({2})", 
                err.key, err.msg, err.errnum);
        }
    }

    static void ShowHelp()
    {
        Console.Error.WriteLine(
            @"Usage: export [-b dbus-moniker] [--dry-run] <outputdir>
  Updates an SQL schema from Versaplex into the outputdir.

  -b: specifies the dbus moniker to connect to.  If not provided, uses
      DBUS_SESSION_BUS_ADDRESS.
  --dry-run: lists the files that would be changed but doesn't modify them.
");
    }

    public static void Main(string[] args)
    {
        WvLog log = new WvLog("Export");

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

        DoExport(bus, exportdir, dry_run);
    }
}
