using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NDesk.DBus;
using Wv;
using Wv.Extensions;

internal static class Schemamatic
{
    static WvLog log = new WvLog("Schemamatic", WvLog.L.Debug2);

    // Returns a blob of text that can be used with PutSchemaData to fill 
    // the given table.
    internal static string GetSchemaData(string clientid, string tablename)
    {
        string query = "SELECT * FROM " + tablename;

        VxColumnInfo[] colinfo;
        object[][] data;
        byte[][] nullity;
        
        VxDb.ExecRecordset(clientid, query, out colinfo, out data, out nullity);

        List<string> cols = new List<string>();
        foreach (VxColumnInfo ci in colinfo)
            cols.Add("[" + ci.ColumnName + "]");

        string prefix = String.Format("INSERT INTO {0} ({1}) VALUES (", 
            tablename, cols.Join(","));

        StringBuilder result = new StringBuilder();
        List<string> values = new List<string>();
        for(int ii = 0; ii < data.Length; ii++)
        {
            object[] row = data[ii];
            values.Clear();
            for (int jj = 0; jj < row.Length; jj++)
            {
                object elem = row[jj];
                VxColumnInfo ci = colinfo[jj];
                log.print("Col {0}, name={1}, type={2}\n", jj, 
                    ci.ColumnName, ci.VxColumnType.ToString());
                if (elem == null)
                    values.Add("NULL");
                else if (ci.VxColumnType == VxColumnType.String ||
                    ci.VxColumnType == VxColumnType.DateTime)
                {
                    // Double-quote chars for SQL safety
                    string esc = elem.ToString().Replace("'", "''");
                    values.Add("'" + esc + "'");
                }
                else
                    values.Add(elem.ToString());
            }
            result.Append(prefix + values.Join(",") + ");\n");
        }

        return result.ToString();
    }

    // Delete all rows from the given table and replace them with the given
    // data.  text is an opaque hunk of text returned from GetSchemaData.
    internal static void PutSchemaData(string clientid, string tablename, 
        string text)
    {
        object result;
        VxDb.ExecScalar(clientid, String.Format("DELETE FROM [{0}]", tablename),
            out result);
        VxDb.ExecScalar(clientid, text, out result);
    }
}
