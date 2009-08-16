/*
 * Versaplex:
 *   Copyright (C)2007-2008 Versabanq Innovations Inc. and contributors.
 *       See the included file named LICENSE for license information.
 */
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Wv
{
    public class WvCsv
    {
        string astext;
        int pos = 0;
        
        public WvCsv(string toparse)
        {
            astext = toparse;
        }
        
        public bool hasMore()
        {
            return (pos < astext.Length);
        }
        
        //returns the next line parsed into an ArrayList
        public List<string> GetLine()
        {
            List<string> asarray = new List<string>();
            
            while (pos < astext.Length)
            {
                if (astext[pos] == '\n')
                {
                    asarray.Add(null);
                    pos++;
                    return asarray;
                }

		StringBuilder field = new StringBuilder();
                
                //certainly a string                
                if (astext[pos] == '"')
                {
		    char lastChar = '"';
		    if (++pos < astext.Length)
		    {
                        field.Append(lastChar = astext[pos]);

			while (++pos < astext.Length)
			{
			    int fminus1 = field.Length - 1;
			    if (lastChar == '"' && (astext[pos] == ',' || 
						    astext[pos] == '\n') &&
				field[fminus1] == '"')
			    {
				string tmp = field.ToString(0, fminus1);
				string temp = tmp.Replace("\"\"", null);
				if ((fminus1 - temp.Length) % 2 == 0 && 
				    !temp.EndsWith("\""))
				{
				    field.Remove(fminus1, 1);
				    break;
				}
			    }
                             
			    field.Append(lastChar = astext[pos]);
			}
		    }

		    int flenminus1 = field.Length - 1;
                    if (pos == astext.Length && astext[pos - 1] != '\n' && 
                        field[flenminus1] == '"')
                        field.Remove(flenminus1, 1);
                    
                    asarray.Add(field.Replace("\"\"","\"").ToString());
                }
                else
                {
                    while (pos < astext.Length && astext[pos] != ',' && 
                           astext[pos] != '\n')
                        field.Append(astext[pos++]);

                    if (field.Length == 0)
                        asarray.Add(null);
                    else
                        asarray.Add(field.Replace("\"\"","\"").ToString());

                }
                if (pos < astext.Length && astext[pos] == '\n')
                {
                    ++pos;
                    return asarray;
                }
                    
                ++pos;
            }

            
            return asarray;
        }
    }
} //namespace
