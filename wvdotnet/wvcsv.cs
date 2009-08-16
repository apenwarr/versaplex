/*
 * Versaplex:
 *   Copyright (C)2007-2008 Versabanq Innovations Inc. and contributors.
 *       See the included file named LICENSE for license information.
 */
using System;
using System.Collections.Generic;
using System.Globalization;

namespace Wv
{
    public class WvCsv
    {
        string astext;
        List<string> asarray;
        int pos = 0;
        
        public WvCsv(string toparse)
        {
            astext = toparse;
        }
        
        public WvCsv(List<string> tounparse)
        {
            asarray = tounparse;
        }
        
        public bool hasMore()
        {
            return (pos < astext.Length);
        }
        
        //return first line of the ArrayList (or the ArrayList) as a string
        public string GetCsvLine()
        {
            return "";
        }
        
        //return the full ArrayList (multiple lines) as CSV
        public string GetCsvText()
        {
            return "";
        }
        
        //returns the next line parsed into an ArrayList
        public List<string> GetLine()
        {
            string field = "";
            asarray = new List<string>();
            
            while (pos < astext.Length)
            {
                if (astext[pos] == '\n')
                {
                    asarray.Add(null);
                    pos++;
                    return asarray;
                }
                
                //certainly a string                
                if (astext[pos] == '"')
                {
		    char lastChar = '"';
                    while (++pos < astext.Length)
                    {
                        if (lastChar == '"' && (astext[pos] == ',' || 
                                                astext[pos] == '\n'))
                        {
                            if (field.EndsWith("\""))
                            {
                                string tmp = field.Substring(0, field.Length-1);
                                string temp = tmp.Replace("\"\"","");
                                if ((tmp.Length - temp.Length) % 2 == 0 && 
                                    !temp.EndsWith("\""))
                                {
                                    field = tmp;
                                    break;
                                }
                            }
                        }
                             
                        field += astext[pos];
                        lastChar = astext[pos];
                    }
                    
                    if (pos == astext.Length && astext[pos-1] != '\n' && 
                        field.EndsWith("\""))
                        field = field.Substring(0, field.Length - 1);
                    
                    asarray.Add(field.Replace("\"\"","\""));
                }
                else
                {
                    while ((pos < astext.Length) && (astext[pos]!=',') && 
                                                    (astext[pos]!='\n'))
                    {
                        field += astext[pos];
                        ++pos;
                    }

                    if (String.IsNullOrEmpty(field))
                        asarray.Add(null);
                    else
                        asarray.Add(field.Replace("\"\"","\""));

                }
                if ((pos < astext.Length) && (astext[pos]=='\n'))
                {
                    ++pos;
                    return asarray;
                }
                    
                field = "";
                ++pos;
            }

            
            return asarray;
        }
    }
} //namespace
