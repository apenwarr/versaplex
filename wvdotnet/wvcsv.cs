using System;
using System.Collections;
using System.Globalization;

namespace Wv
{
    public class WvCsv
    {
        string astext;
        ArrayList asarray;
        int pos = 0;
        
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
        public ArrayList GetLine()
        {
            int lengthBefore = 0;
            char lastChar;
            string tmp = "";
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
                    while ((pos < astext.Length) && ((lastChar!='"') &&
                                     ( (astext[pos]!=',') || (astext[pos]!='\n') ) ))
                    {
                        tmp += astext[pos];
                        lastChar = astext[pos];
                        pos++;
                    }
                    
                    if (tmp.EndsWith("\""))
                        tmp = tmp.Substring(0,tmp.Length-1);
                        
                    lengthBefore = tmp.Length;
                    tmp = tmp.Replace("\"\"","\"");
                    if (((lengthBefore-tmp.Length)/2) != 0)
                        Console.WriteLine("Warning: unescaped double-quotes found " +
                                          "near position {0}", pos);
                    
                    asarray.Add(tmp);
                }
                else
                {
                    while ((pos < astext.Length) && (astext[pos]!=',') && 
                                                    (astext[pos]!='\n'))
                    {
                        tmp += astext[pos];
                        pos++;
                    }

                    if (String.IsNullOrEmpty(tmp))
                        asarray.Add(null);
                    else
                        asarray.Add(tmp.Replace("\"\"","\""));

                }
                if ((pos < astext.Length) && (astext[pos]=='\n'))
                {
                    pos++;
                    return asarray;
                }
                    
                tmp = "";
                pos++;
            }
            
            return asarray;
        }
    }
} //namespace
