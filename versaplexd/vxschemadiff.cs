using System;
using System.Linq;
using System.Text;
using System.Collections.Generic;

internal enum VxDiffType
{
    Unchanged = '.',
    Add = '+',
    Remove = '-',
    Change = '*'
}

// Figures out what changes are needed to convert srcsums to goalsums.
//
// FIXME: It might be nicer in the long term to just implement 
// IEnumerable<...> or IDictionary<...> ourselves, and defer to
// an internal member.  But it's a lot of boilerplate code.
internal class VxSchemaDiff : SortedList<string, VxDiffType>
{
    public VxSchemaDiff(VxSchemaChecksums srcsums, 
        VxSchemaChecksums goalsums):
        base(new SchemaTypeComparer())
    {
        List<string> keys = srcsums.Keys.Union(goalsums.Keys).ToList();
        keys.Sort(new SchemaTypeComparer());
        foreach (string key in keys)
        {
            if (!srcsums.ContainsKey(key))
                this.Add(key, VxDiffType.Add);
            else if (!goalsums.ContainsKey(key))
                this.Add(key, VxDiffType.Remove);
            else if (!srcsums[key].Equals(goalsums[key]))
            {
                if (!this.ContainsKey(key))
                    this.Add(key, VxDiffType.Change);
            }
            else
            {
                //this.Add(key, VxDiffType.Unchanged);
            }
        }
    }

    // Convert a set of diffs to a string of the form:
    // + AddedEntry
    // - RemovedEntry
    // * ChangedEntry
    // . UnchangedEntry
    // The leading characters are taken directly from the enum definition.
    public override string ToString()
    {
        StringBuilder sb = new StringBuilder();
        // Assume around 32 characters per entry.  May be slightly off, but
        // it'll be way better than the default value of 16.
        sb.Capacity = 32 * this.Count;
        foreach (KeyValuePair<string,VxDiffType> p in this)
        {
            sb.AppendLine(((char)p.Value) + " " + p.Key); 
        }
        return sb.ToString();
    }
}

internal class SchemaTypeComparer: IComparer<string>
{
    enum SchemaTypes
    {
        xmlschema = 100,
        table = 200,
        view = 300,
        index = 400,
        scalarfunction = 1100,
        tablefunction = 1200,
        procedure = 1300,
        trigger = 1400
    }

    private int sort_order(string s)
    {
        string type, name;
        VxSchema.ParseKey(s, out type, out name);

        int retval;
        bool ignore_case = true;
        try
        {
            retval = Convert.ToInt32(Enum.Parse(typeof(SchemaTypes), 
                type, ignore_case));
        }
        catch (Exception)
        {
            retval = 9999;
        }
        return retval;
    }

    public int Compare(string x, string y)
    {
        int sort_x = sort_order(x);
        int sort_y = sort_order(y);

        if (sort_x != sort_y)
            return sort_x - sort_y;
        else
            return String.Compare(x, y);
    }
}

