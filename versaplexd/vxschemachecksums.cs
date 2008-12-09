/*
 * Versaplex:
 *   Copyright (C)2007-2008 Versabanq Innovations Inc. and contributors.
 *       See the included file named LICENSE for license information.
 */
using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Security.Cryptography;
using Wv;
using Wv.Extensions;

// The checksums for a single database element (table, procedure, etc).
// Can have multiple checksum values - a table has one checksum per column,
// for instance.
// Note: This class is immutable.  
// FIXME: Have separate type member?
internal class VxSchemaChecksum
{
    readonly string _key;
    public string key {
        get { return _key; }
    }

    readonly IEnumerable<ulong> _checksums;
    public IEnumerable<ulong> checksums {
        get { return _checksums; }
    }

    public VxSchemaChecksum(VxSchemaChecksum copy)
    {
        _key = copy.key;
        var list = new List<ulong>();
        foreach (ulong sum in copy.checksums)
            list.Add(sum);
        _checksums = list;
    }

    public VxSchemaChecksum(string newkey)
    {
        _key = newkey;
        _checksums = new List<ulong>();
    }

    public VxSchemaChecksum(string newkey, ulong newchecksum)
    {
        _key = newkey;
        var list = new List<ulong>();
        list.Add(newchecksum);
        _checksums = list;
    }

    public VxSchemaChecksum(string newkey, IEnumerable<ulong> sumlist)
    {
        _key = newkey;

        // Tables need to maintain their checksums in sorted order, as the
        // columns might get out of order when the tables are otherwise
        // identical.
        string type, name;
        VxSchemaChecksums.ParseKey(newkey, out type, out name);
        if (type == "Table")
        {
            List<ulong> sorted = sumlist.ToList();
            sorted.Sort();
            _checksums = sorted;
        }
        else
            _checksums = sumlist;
    }
    
    public string GetSumString()
    {
        List<string> l = new List<string>();
        foreach (ulong sum in checksums)
            l.Add("0x" + sum.ToString("x8"));
        return l.join(" ");
    }

    // Write the checksum values to DBus
    public void Write(WvDbusWriter writer)
    {
        writer.Write(key);
	writer.WriteArray(8, _checksums, (w2, sum) => {
	    w2.Write(sum);
	});
    }

    // Note: this is only safe to override because the class is immutable.
    public override bool Equals(object other_obj)
    {
        if (other_obj == null)
            return false;

        if (!(other_obj is VxSchemaChecksum))
            return false;

        var other = (VxSchemaChecksum)other_obj;

        if (this.key != other.key)
            return false;

        // FIXME: This can be replaced with Linq's SequenceEquals(), once
        // we're using a version of mono that implements it (>= 1.9).
        ulong[] mysums = this.checksums.ToArray();
        ulong[] theirsums = other.checksums.ToArray();

        if (mysums.Count() != theirsums.Count())
            return false;

        for (int i = 0; i < mysums.Count(); i++)
            if (mysums[i] != theirsums[i])
                return false;

        return true;
    }

    public override int GetHashCode() 
    {
        ulong xor = checksums.Aggregate((cur, next) => cur ^ next);
        return ((int)xor ^ (int)(xor>>32));
    }

    // Given a string containing database sums, returns their parsed version.
    // Leading "0x" prefixes are optional, though all numbers are assumed to
    // be hex.  The sums must be separated by spaces, e.g. "0xdeadbeef badf00d"
    // Prints an error message and ignores any unparseable elements.
    public static IEnumerable<ulong> ParseSumString(string dbsums)
    {
        return ParseSumString(dbsums, null);
    }

    // Acts just like ParseSumString(dbsums), but allows for providing 
    // some context for errors.  If errctx isn't null, it's printed before any
    // error messages produced.  A suitable string might be "Error while 
    // reading file $filename: ".
    public static IEnumerable<ulong> ParseSumString(string dbsums, 
        string errctx)
    {
        if (dbsums == null)
            return new List<ulong>();

        string[] sums = dbsums.Split(' ');
        var sumlist = new List<ulong>();
        foreach (string sumstr in sums)
        {
            // Ignore trailing spaces.
            if (sumstr.Length == 0)
                continue;

            // C#'s hex parser doesn't like 0x prefixes.
            string stripped = sumstr.ToLower();
            if (stripped.StartsWith("0x"))
                stripped = stripped.Remove(0, 2);

            ulong longsum;
            if (UInt64.TryParse(stripped,
                    System.Globalization.NumberStyles.HexNumber, null, 
                    out longsum))
            {
                sumlist.Add(longsum);
            }
            else
            {
                WvLog log = new WvLog("ParseSumString");
                string msg = wv.fmt("Failed to parse database sums '{0}' " + 
                    "due to the malformed element '{1}'.\n", 
                    dbsums, sumstr);
                log.print("{0}{1}", errctx == null ? "" : errctx, msg);
            }
        }
        return sumlist;
    }

    public static string GetDbusSignature()
    {
        return "sat";
    }
}

// The checksum values for a set of database elements
internal class VxSchemaChecksums : Dictionary<string, VxSchemaChecksum>
{
    public VxSchemaChecksums()
    {
    }

    public VxSchemaChecksums(VxSchemaChecksums copy)
    {
        foreach (KeyValuePair<string,VxSchemaChecksum> p in copy)
            this.Add(p.Key, new VxSchemaChecksum(p.Value));
    }

    // Read an array of checksums from a DBus message.
    // Signature: a(sat)
    public VxSchemaChecksums(WvDbusMsg reply)
    {
	var array = reply.iter().pop();
	
	foreach (WvAutoCast i in array)
	{
	    var ii = i.GetEnumerator();
	    string key = ii.pop();
	    var sums = ii.pop().Cast<UInt64>();
	    var cs = new VxSchemaChecksum(key, sums);
	    Add(cs.key, cs);
	}
    }

    // Write the list of checksums to DBus in a(sat) format.
    public void WriteChecksums(WvDbusWriter writer)
    {
	writer.WriteArray(8, this, (w2, p) => {
	    p.Value.Write(w2);
	});
    }

    public void AddSum(string key, ulong checksum)
    {
        if (this.ContainsKey(key))
        {
            VxSchemaChecksum old = this[key];

            List<ulong> list = new List<ulong>(old.checksums);
            list.Add(checksum);

            this[key] = new VxSchemaChecksum(key, list);
        }
        else
            this.Add(key, new VxSchemaChecksum(key, checksum));
    }

    public static string GetDbusSignature()
    {
        return String.Format("a({0})", VxSchemaChecksum.GetDbusSignature());
    }

    public static void ParseKey(string key, out string type, out string name)
    {
        string[] parts = key.split("/", 2);
        if (parts.Length != 2)
        {
            type = name = null;
            return;
        }
        type = parts[0];
        name = parts[1];
        return;
    }
}

