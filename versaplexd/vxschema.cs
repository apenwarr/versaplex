using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Security.Cryptography;
using Wv;
using Wv.Extensions;

[Flags]
public enum VxCopyOpts : int
{
    None = 0,
    DryRun = 0x1,
    ShowProgress = 0x2, 
    ShowDiff = 0x4, 
    Destructive = 0x8,

    Verbose = ShowProgress | ShowDiff,
}

internal class VxSchemaElement : IComparable
{
    string _type;
    public string type {
        get { return _type; }
    }

    string _name;
    public string name {
        get { return _name; }
    }

    string _text;
    public virtual string text {
        get { return _text; }
        set { _text = value;}
    }
    
    bool _encrypted;
    public bool encrypted {
        get { return _encrypted; }
    }

    public string key {
        get { return type + "/" + name; }
    }

    public VxSchemaElement(string newtype, string newname, string newtext,
            bool newencrypted)
    {
        _type = newtype;
        _name = newname;
        _encrypted = newencrypted;
        _text = newtext;
    }

    public VxSchemaElement(VxSchemaElement copy)
    {
        _type = copy.type;
        _name = copy.name;
        _text = copy.text;
        _encrypted = copy.encrypted;
    }

    public VxSchemaElement(MessageReader reader)
    {
        _type = reader.ReadString();
        _name = reader.ReadString();
        _text = reader.ReadString();
        _encrypted = reader.ReadByte() > 0;
    }

    public void Write(MessageWriter writer)
    {
        writer.Write(typeof(string), type);
        writer.Write(typeof(string), name);
        writer.Write(typeof(string), text);
        byte encb = (byte)(encrypted ? 1 : 0);
        writer.Write(typeof(byte), encb);
    }

    public string GetKey()
    {
        return VxSchema.GetKey(type, name, encrypted);
    }

    // It's not guaranteed that the text field will be valid SQL.  Give 
    // subclasses a chance to translate.
    public virtual string ToSql()
    {
        return this.text;
    }

    // Returns the element's text, along with a header line containing the MD5
    // sum of the text, and the provided database checksums.  This format is
    // suitable for serializing to disk.
    public string ToStringWithHeader(VxSchemaChecksum sum)
    {
        byte[] md5 = MD5.Create().ComputeHash(text.ToUTF8());

        return String.Format("!!SCHEMAMATIC {0} {1} \r\n{2}",
            md5.ToHex().ToLower(), sum.GetSumString(), text);
    }

    public int CompareTo(object obj)
    {
        if (!(obj is VxSchemaElement))
            throw new ArgumentException("object is not a VxSchemaElement");

        VxSchemaElement other = (VxSchemaElement)obj;
        return GetKey().CompareTo(other.GetKey());
    }

    public static string GetDbusSignature()
    {
        return "sssy";
    }
}

// Represents an element of a table, such as a column or index.
// Each element has a type, such as "column", and a series of key,value
// parameters, such as ("name","MyColumn"), ("type","int"), etc.
internal class VxSchemaTableElement
{
    public string elemtype;
    // We can't use a Dictionary<string,string> because we might have repeated
    // keys (such as two columns for an index).
    public List<KeyValuePair<string,string>> parameters;

    public VxSchemaTableElement(string type)
    {
        elemtype = type;
        parameters = new List<KeyValuePair<string,string>>();
    }

    public void AddParam(string name, string val)
    {
        parameters.Add(new KeyValuePair<string,string>(name, val));
    }

    // Returns the first parameter found with the given name.
    // Returns an empty string if none found.
    public string GetParam(string name)
    {
        foreach (var kvp in parameters)
            if (kvp.Key == name)
                return kvp.Value;
        return "";
    }

    // Returns a list of all parameters found with the given name.
    // Returns an empty list if none found.
    public List<string> GetParamList(string name)
    {
        List<string> results = new List<string>();
        foreach (var kvp in parameters)
            if (kvp.Key == name)
                results.Add(kvp.Value);
        return results;
    }

    // Serializes to "elemtype: key1=value1,key2=value2" format.
    public override string ToString()
    {
        StringBuilder sb = new StringBuilder();
        bool empty = true;
        sb.Append(elemtype + ": ");
        foreach (var param in parameters)
        {
            if (!empty)
                sb.Append(",");

            sb.Append(param.Key + "=" + param.Value);

            empty = false;
        }
        return sb.ToString();
    }

    public static int CompareTableElemsByName(VxSchemaTableElement e1, 
        VxSchemaTableElement e2) 
    { 
        return e1.GetParam("name").CompareTo(e2.GetParam("name"));
    }
}

internal class VxSchemaTable : VxSchemaElement
{
    private List<VxSchemaTableElement> elems;

    public VxSchemaTable(string newname) :
        base("Table", newname, null, false)
    {
        elems = new List<VxSchemaTableElement>();
    }

    public VxSchemaTable(string newname, string newtext) :
        base("Table", newname, null, false)
    {
        elems = new List<VxSchemaTableElement>();
        // Parse the new text
        text = newtext;
    }

    public VxSchemaTable(VxSchemaElement elem) :
        base("Table", elem.name, null, false)
    {
        elems = new List<VxSchemaTableElement>();
        // Parse the new text
        text = elem.text;
    }

    // Internal function to be used in unit tests.  You shouldn't call this.
    internal IEnumerator<VxSchemaTableElement> GetElems()
    {
        return elems.GetEnumerator();
    }

    public override string text
    {
        // Other schema elements just store their text verbatim.
        // We parse it on input and recreate it on output in order to 
        // provide more sensible updating of tables in the database.
        get
        {
            StringBuilder sb = new StringBuilder();
            foreach (var elem in elems)
                sb.Append(elem.ToString() + "\n");
            return sb.ToString();
        }
        set
        {
            elems.Clear();
            char[] equals = {'='};
            char[] comma = {','};
            foreach (string line in value.Split('\n'))
            {
                line.Trim();
                if (line.Length == 0)
                    continue;

                string typeseparator = ": ";
                int index = line.IndexOf(typeseparator);
                if (index < 0)
                    throw new ArgumentException("Malformed line: " + line);
                string type = line.Remove(index);
                string rest = line.Substring(index + typeseparator.Length);

                var elem = new VxSchemaTableElement(type);

                foreach (string kvstr in rest.Split(comma))
                {
                    string[] kv = kvstr.Split(equals, 2);
                    if (kv.Length != 2)
                        throw new ArgumentException(wv.fmt(
                            "Invalid entry '{0}' in line '{1}'",
                            kvstr, line));

                    elem.parameters.Add(
                        new KeyValuePair<string,string>(kv[0], kv[1]));
                }
                elems.Add(elem);
            }
        }
    }

    public string ColumnToSql(VxSchemaTableElement elem)
    {
        string colname = elem.GetParam("name");
        string typename = elem.GetParam("type");
        string lenstr = elem.GetParam("length");
        string defval = elem.GetParam("default");
        string nullstr = elem.GetParam("null");
        string prec = elem.GetParam("precision");
        string scale = elem.GetParam("scale");
        string ident_seed = elem.GetParam("identity_seed");
        string ident_incr = elem.GetParam("identity_incr");

        string identstr = "";
        if (!String.IsNullOrEmpty(ident_seed) && 
            !String.IsNullOrEmpty(ident_incr))
        {
            identstr = wv.fmt(" IDENTITY ({0},{1})", 
                ident_seed, ident_incr);
        }

        if (String.IsNullOrEmpty(nullstr))
            nullstr = "";
        else if (nullstr == "0")
            nullstr = " NOT NULL";
        else
            nullstr = " NULL";

        if (!String.IsNullOrEmpty(lenstr))
            lenstr = " (" + lenstr + ")";
        else if (!String.IsNullOrEmpty(prec) && 
            !String.IsNullOrEmpty(scale))
        {
            lenstr = wv.fmt(" ({0},{1})", prec, scale);
        }

        if (!String.IsNullOrEmpty(defval))
            defval = " DEFAULT " + defval;

        return wv.fmt("[{0}] [{1}]{2}{3}{4}{5}",
            colname, typename, lenstr, defval, nullstr, identstr);
    }

    public string IndexToSql(VxSchemaTableElement elem)
    {
        List<string> idxcols = elem.GetParamList("column");
        string idxname = elem.GetParam("name");
        string unique = elem.GetParam("unique");
        string clustered = elem.GetParam("clustered") == "1" ? 
            "CLUSTERED " : "";

        if (unique != "" && unique != "0")
            unique = "UNIQUE ";
        else
            unique = "";

        return wv.fmt(
            "CREATE {0}{1}INDEX [{2}] ON [{3}] \n\t({4});",
            unique, clustered, idxname, this.name, idxcols.Join(", "));
    }

    public string PrimaryKeyToSql(VxSchemaTableElement elem)
    {
        List<string> idxcols = elem.GetParamList("column");
        string idxname = elem.GetParam("name");
        string clustered = elem.GetParam("clustered") == "1" ? 
            " CLUSTERED" : " NONCLUSTERED";

        // If no name is specified, set it to "PK_TableName"
        if (String.IsNullOrEmpty(idxname))
            idxname = "PK_" + this.name;

        return wv.fmt(
            "ALTER TABLE [{0}] ADD CONSTRAINT [{1}] PRIMARY KEY{2}\n" +
            "\t({3});\n\n", 
            this.name, idxname, clustered, idxcols.Join(", "));
    }

    public override string ToSql()
    {
        List<string> cols = new List<string>();
        List<string> indexes = new List<string>();
        string pkey = "";
        foreach (var elem in elems)
        {
            if (elem.elemtype == "column")
                cols.Add(ColumnToSql(elem));
            else if (elem.elemtype == "index")
                indexes.Add(IndexToSql(elem));
            else if (elem.elemtype == "primary-key")
            {
                if (pkey != "")
                {
                    throw new VxBadSchemaException(
                        "Multiple primary key statements are not " + 
                        "permitted in table definitions.\n" + 
                        "Conflicting statement: " + elem.ToString() + "\n");
                }
                pkey = PrimaryKeyToSql(elem);
            }
        }

        string table = String.Format("CREATE TABLE [{0}] (\n\t{1});\n\n{2}{3}\n",
            name, cols.Join(",\n\t"), pkey, indexes.Join("\n"));
        return table;
    }

    public void AddColumn(string name, string type, int isnullable, 
        string len, string defval, string prec, string scale, 
        int isident, string ident_seed, string ident_incr)
    {
        var elem = new VxSchemaTableElement("column");
        // FIXME: Put the table name here or not?  Might be handy, but could
        // get out of sync with e.g. filename or whatnot.
        elem.AddParam("name", name);
        elem.AddParam("type", type);
        elem.AddParam("null", isnullable.ToString());
        if (!String.IsNullOrEmpty(len))
            elem.AddParam("length", len);
        if (!String.IsNullOrEmpty(defval))
            elem.AddParam("default", defval);
        if (!String.IsNullOrEmpty(prec))
            elem.AddParam("precision", prec);
        if (!String.IsNullOrEmpty(scale))
            elem.AddParam("scale", scale);
        if (isident != 0)
        {
            elem.AddParam("identity_seed", ident_seed);
            elem.AddParam("identity_incr", ident_incr);
        }
        elems.Add(elem);
    }

    public void AddIndex(string name, int unique, int clustered, 
        params string[] columns)
    {
        WvLog log = new WvLog("AddIndex");
        log.print("Adding index on {0}, name={1}, unique={2}, clustered={3},\n", 
            columns.Join(","), name, unique, clustered);
        var elem = new VxSchemaTableElement("index");

        foreach (string col in columns)
            elem.AddParam("column", col);
        elem.AddParam("name", name);
        elem.AddParam("unique", unique.ToString());
        elem.AddParam("clustered", clustered.ToString());

        elems.Add(elem);
    }

    public void AddPrimaryKey(string name, int clustered, 
        params string[] columns)
    {
        WvLog log = new WvLog("AddPrimaryKey");
        log.print("Adding primary key '{0}' on {1}, clustered={2}\n", 
            name, columns.Join(","), clustered);
        var elem = new VxSchemaTableElement("primary-key");

        if (!String.IsNullOrEmpty(name) && name != "PK_" + this.name)
            elem.AddParam("name", name);

        foreach (string col in columns)
            elem.AddParam("column", col);
        elem.AddParam("clustered", clustered.ToString());

        elems.Add(elem);
    }

    // Figure out what changed between oldtable and newtable.  Returns a 
    // dictionary mapping table element types and names (if applicable) to 
    // diff types.  The keys are of the form "column: ColumnName" or 
    // "index: IndexName", or just "primary-key" for primary keys, since
    // there's only one primary key at a time.
    public static Dictionary<VxSchemaTableElement, VxDiffType> GetDiff(
        VxSchemaTable oldtable, VxSchemaTable newtable)
    {
        WvLog log = new WvLog("SchemaTable GetDiff");
        var diff = new Dictionary<VxSchemaTableElement, VxDiffType>();
        var oldset = new Dictionary<string, VxSchemaTableElement>();
        var newset = new Dictionary<string, VxSchemaTableElement>();

        // Build some dictionaries so we can quickly check if a particular 
        // entry exists in a table, even if its attributes have changed.
        // We only have one primary key per table, no need to distinguish it
        // further (and it might not have a name yet).  But of course there'll
        // be multiple columns and indexes, so we need to key off of the
        // element type and name.
        foreach (var elem in oldtable.elems)
        {
            string key;
            if (elem.elemtype == "primary-key")
                key = elem.elemtype;
            else
                key = elem.elemtype + ": " + elem.GetParam("name");

            if (oldset.ContainsKey(key))
                throw new VxBadSchemaException(wv.fmt("Duplicate table entry " + 
                    "'{0}' found.", key));

            oldset.Add(key, elem);
        }
        foreach (var elem in newtable.elems)
        {
            string key;
            if (elem.elemtype == "primary-key")
                key = elem.elemtype;
            else
                key = elem.elemtype + ": " + elem.GetParam("name");

            if (newset.ContainsKey(key))
                throw new VxBadSchemaException(wv.fmt("Duplicate table entry " + 
                    "'{0}' found.", key));

            newset.Add(key, elem);
        }

        foreach (var kvp in oldset)
        {
            if (!newset.ContainsKey(kvp.Key))
            {
                log.print("Scheduling {0} for removal.\n", kvp.Key);
                diff.Add(kvp.Value, VxDiffType.Remove);
            }
            else if (kvp.Value.ToString() != newset[kvp.Key].ToString())
            {
                log.print("Scheduling {0} for change.\n", kvp.Key);
                string elemtype = kvp.Value.elemtype;
                if (elemtype == "primary-key" || elemtype == "index")
                {
                    // Don't bother trying to change indexes, just delete and
                    // re-add them.  Makes it possible to rename primary keys.
                    diff.Add(kvp.Value, VxDiffType.Remove);
                    diff.Add(newset[kvp.Key], VxDiffType.Add);
                }
                else
                    diff.Add(newset[kvp.Key], VxDiffType.Change);
            }
        }
        foreach (var kvp in newset)
        {
            if (!oldset.ContainsKey(kvp.Key))
            {
                log.print("Scheduling {0} for addition.\n", kvp.Key);
                diff.Add(kvp.Value, VxDiffType.Add);
            }
        }

        return diff;
    }
}

// The schema elements for a set of database elements
internal class VxSchema : Dictionary<string, VxSchemaElement>
{
    public static ISchemaBackend create(string moniker)
    {
	ISchemaBackend sm = WvMoniker<ISchemaBackend>.create(moniker);
	if (sm == null && Directory.Exists(moniker))
	    sm = WvMoniker<ISchemaBackend>.create("dir:" + moniker);
	if (sm == null)
	    sm = WvMoniker<ISchemaBackend>.create("dbi:" + moniker);
	if (sm == null)
	    throw new Exception
	    (wv.fmt("No moniker found for '{0}'", moniker));
	return sm;
    }
    
    public VxSchema()
    {
    }

    // Convenience method for making single-element schemas
    public VxSchema(VxSchemaElement elem)
    {
        Add(elem.key, elem);
    }

    public VxSchema(VxSchema copy)
    {
        foreach (KeyValuePair<string,VxSchemaElement> p in copy)
            this.Add(p.Key, new VxSchemaElement(p.Value));
    }

    public VxSchema(MessageReader reader)
    {
	reader.ReadArrayFunc(8, (r) => {
            VxSchemaElement elem = new VxSchemaElement(r);
            if (elem.type == "Table")
                elem = new VxSchemaTable(elem);
            Add(elem.GetKey(), elem);
	});
    }

    private void _WriteSchema(MessageWriter writer)
    {
        foreach (KeyValuePair<string,VxSchemaElement> p in this)
        {
            writer.WritePad(8);
            p.Value.Write(writer);
        }
    }

    public void WriteSchema(MessageWriter writer)
    {
        writer.WriteDelegatePrependSize(delegate(MessageWriter w)
            {
                _WriteSchema(w);
            }, 8);
    }

    // Returns only the elements of the schema that are affected by the diff.
    // If an element is scheduled to be removed, clear its text field.
    // Produces a VxSchema that, if sent to a schema backend's Put, will
    // update the schema as indicated by the diff.
    public VxSchema GetDiffElements(VxSchemaDiff diff)
    {
        VxSchema diffschema = new VxSchema();
        foreach (KeyValuePair<string,VxDiffType> p in diff)
        {
            if (!this.ContainsKey(p.Key))
                throw new ArgumentException("The provided diff does not " + 
                    "match the schema: extra element '" + 
                    (char)p.Value + " " + p.Key + "'");
            if (p.Value == VxDiffType.Remove)
            {
                VxSchemaElement elem = new VxSchemaElement(this[p.Key]);
                elem.text = "";
                diffschema[p.Key] = elem;
            }
            else if (p.Value == VxDiffType.Add || p.Value == VxDiffType.Change)
            {
                diffschema[p.Key] = new VxSchemaElement(this[p.Key]);
            }
        }
        return diffschema;
    }

    public void Add(string type, string name, string text, bool encrypted)
    {
        string key = GetKey(type, name, encrypted);
        if (this.ContainsKey(key))
            this[key].text += text;
        else
        {
            if (type == "Table")
                this.Add(key, new VxSchemaTable(name, text));
            else
                this.Add(key, new VxSchemaElement(type, name, text, encrypted));
        }
    }

    public static string GetKey(string type, string name, bool encrypted)
    {
        string enc_str = encrypted ? "-Encrypted" : "";
        return String.Format("{0}{1}/{2}", type, enc_str, name);
    }

    // ParseKey used to live here, but moved to VxSchemaChecksums.  
    public static void ParseKey(string key, out string type, out string name)
    {
        VxSchemaChecksums.ParseKey(key, out type, out name);
        return;
    }

    public static string GetDbusSignature()
    {
        return String.Format("a({0})", VxSchemaElement.GetDbusSignature());
    }

    // Make dest look like source.  Only copies the bits that need updating.
    // Note: this is a slightly funny spot to put this method; it really
    // belongs in ISchemaBackend, but you can't put methods in interfaces.
    public static VxSchemaErrors CopySchema(ISchemaBackend source, 
        ISchemaBackend dest)
    {
        return VxSchema.CopySchema(source, dest, VxCopyOpts.None);
    }

    public static VxSchemaErrors CopySchema(ISchemaBackend source, 
        ISchemaBackend dest, VxCopyOpts opts)
    {
        WvLog log = new WvLog("CopySchema");

        if ((opts & VxCopyOpts.ShowProgress) == 0)
            log = new WvLog("CopySchema", WvLog.L.Debug5);

        bool show_diff = (opts & VxCopyOpts.ShowDiff) != 0;
        bool dry_run = (opts & VxCopyOpts.DryRun) != 0;
        bool destructive = (opts & VxCopyOpts.Destructive) != 0;

        log.print("Retrieving schema checksums from source.\n");
        VxSchemaChecksums srcsums = source.GetChecksums();

        log.print("Retrieving schema checksums from dest.\n");
        VxSchemaChecksums destsums = dest.GetChecksums();

        if (srcsums.Count == 0 && destsums.Count != 0)
        {
            log.print("Source index is empty! " + 
                "Refusing to delete entire database.\n");
            return new VxSchemaErrors();
        }

        List<string> names = new List<string>();

        log.print("Computing diff.\n");
        VxSchemaDiff diff = new VxSchemaDiff(destsums, srcsums);

        if (diff.Count == 0)
        {
            log.print("No changes.\n");
            return new VxSchemaErrors();
        }

        if (show_diff)
        {
            log.print("Changes to apply:\n");
            log.print(WvLog.L.Info, diff.ToString());
        }

        log.print("Parsing diff.\n");
        List<string> to_drop = new List<string>();
        foreach (KeyValuePair<string,VxDiffType> p in diff)
        {
            switch (p.Value)
            {
            case VxDiffType.Remove:
                to_drop.Add(p.Key);
                break;
            case VxDiffType.Add:
            case VxDiffType.Change:
                names.Add(p.Key);
                break;
            }
        }

        log.print("Retrieving updated schema.\n");
        VxSchema to_put = source.Get(names);

        if (dry_run)
            return new VxSchemaErrors();

        VxSchemaErrors drop_errs = new VxSchemaErrors();
        VxSchemaErrors put_errs = new VxSchemaErrors();

        // We know at least one of to_drop and to_put must have something in
        // it, otherwise the diff would have been empty.

        if (to_drop.Count > 0)
        {
            log.print("Dropping deleted elements.\n");
            drop_errs = dest.DropSchema(to_drop);
        }

        VxPutOpts putopts = VxPutOpts.None;
        if (destructive)
            putopts |= VxPutOpts.Destructive;
        if (names.Count > 0)
        {
            log.print("Updating and adding elements.\n");
            put_errs = dest.Put(to_put, srcsums, putopts);
        }

        // Combine the two sets of errors.
        foreach (var kvp in drop_errs)
            put_errs.Add(kvp.Key, kvp.Value);

        return put_errs;
    }
}
