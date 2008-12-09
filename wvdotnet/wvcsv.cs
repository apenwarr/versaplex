/*
 * Versaplex:
 *   Copyright (C)2007-2008 Versabanq Innovations Inc. and contributors.
 *       See the included file named LICENSE for license information.
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using Wv.Extensions;

namespace Wv
{
    public class WvCsv
    {
        string astext;
        ArrayList asarray;
        int pos = 0;
        int line = 0;
        
        public static bool RequiresQuotes(string text)
        {
            return ((text.IndexOf(" ") >= 0) || 
                    (text.IndexOf(",") >= 0) ||
                    (text.IndexOf("\n") >= 0) || 
                    (text.IndexOf("\"") >= 0) ||
                    (text.Length == 0) );
        }
        
        public WvCsv(string toparse)
        {
            astext = toparse;
        }
        
        public WvCsv(ArrayList tounparse)
        {
            asarray = tounparse;
        }
        
        public bool hasMore()
        {
            return (pos < astext.Length);
        }
        
        //return first line of the ArrayList (or the ArrayList) as a CSV string
        public string GetCsvLine()
        {
            int tmp = 0;
            foreach (object item in asarray)
            {
                if (tmp == line)
                {
                    line++;
                    if (item is ArrayList)
                        return GetCsvLine((ArrayList)item);
                    else
                        return GetCsvLine(asarray);
                }
                        
                tmp++;
            }
            line++;
            return "";
        }
        
        public static string GetCsvLine(ArrayList al)
        {
            string[] csvarray = (string[])al.ToArray(Type
                                                 .GetType("System.String"));
            List<string> values = new List<string>();
            string tmp;
            
            for (int i=0; i<csvarray.Length; i++)
            {
                if (csvarray[i] == null)
                    values.Add("");
                else
                {
                    tmp = csvarray[i];
                    tmp = tmp.Replace("'","''");
                    tmp = tmp.Replace("\"","\"\"");                
                    
                    if (RequiresQuotes(tmp))
                        values.Add('"' + tmp + '"'); 
                    else
                        values.Add(tmp);
                }
            }
            return values.join(",");
        }
        
        //return the full ArrayList (multiple lines) as CSV
        public string GetCsvText()
        {
            string tmp = "";
            for (int i=0; i<asarray.Count; i++)
                tmp += GetCsvLine()+"\n";
            return tmp;
        }
        
        //returns the next line parsed into an ArrayList
        public ArrayList GetLine()
        {
            char lastChar;
            string field = "";
            string tmp = "";
            string temp;
            asarray = new ArrayList();
            
            while (pos < astext.Length)
            {
                if (astext[pos] == '\n')
                {
                    asarray.Add(null);
                    pos++;
                    return asarray;
                }
                
                lastChar = ' ';

                //certainly a string                
                if (astext[pos] == '"')
                {
                    pos++;
                    lastChar = '"';
                    while (pos < astext.Length)
                    {
                        if ((lastChar=='"') && ((astext[pos]==',') || 
                                                (astext[pos]=='\n')) )
                        {
                            if (field.EndsWith("\""))
                            {
                                tmp = field.Substring(0,field.Length-1);
                                temp = tmp.Replace("\"\"","");
                                if (((tmp.Length - temp.Length) %2 == 0) && 
                                    (!temp.EndsWith("\"")))
                                {
                                    field = tmp;
                                    break;
                                }
                            }
                        }
                             
                        field += astext[pos];
                        lastChar = astext[pos];
                        pos++;
                    }
                    
                    if ((pos==astext.Length) && (astext[pos-1]!='\n') && 
                                                field.EndsWith("\""))
                        field = field.Substring(0,field.Length-1);
                    
                    asarray.Add(field.Replace("\"\"","\""));
                }
                else
                {
                    while ((pos < astext.Length) && (astext[pos]!=',') && 
                                                    (astext[pos]!='\n'))
                    {
                        field += astext[pos];
                        pos++;
                    }

                    if (String.IsNullOrEmpty(field))
                        asarray.Add(null);
                    else
                        asarray.Add(field.Replace("\"\"","\""));

                }
                if ((pos < astext.Length) && (astext[pos]=='\n'))
                {
                    pos++;
                    return asarray;
                }
                    
                field = "";
                pos++;
            }

            
            return asarray;
        }
    }
} //namespace
