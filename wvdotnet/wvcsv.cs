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
	public WvCsv()
	{
	}
        
	public List<string> GetOne(string line)
	{
	    List<string> asarray = new List<string>();
	    int pos = 0;
            while (pos < line.Length)
            {
                if (line[pos] == '\n')
                {
                    asarray.Add(null);
                    pos++;
		    continue;
                }

		StringBuilder field = new StringBuilder();
                //certainly a string                
                if (line[pos] == '"')
                {
		    if (++pos < line.Length)
		    {
                        field.Append(line[pos]);

			while (++pos < line.Length)
			{
			    int fminus1 = field.Length - 1;
			    if ((line[pos] == ',' || line[pos] == '\n') &&
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
                             
			    field.Append(line[pos]);
			}
		    }

		    int flenminus1 = field.Length - 1;
                    if (pos == line.Length && line[pos - 1] != '\n' && 
                        field[flenminus1] == '"')
                        field.Remove(flenminus1, 1);
                    
                    asarray.Add(field.Replace("\"\"","\"").ToString());
                }
                else
                {
                    while (pos < line.Length && line[pos] != ',' && 
                           line[pos] != '\n')
                        field.Append(line[pos++]);

                    if (field.Length == 0)
                        asarray.Add(null);
                    else
                        asarray.Add(field.Replace("\"\"","\"").ToString());

                }

                ++pos;
            }
            return asarray;
	}
        
        public IEnumerable< List<string> > GetLines(WvInBufStream text)
        {
	    char[] totrim = {'\r', '\n'};
	    StringBuilder csvtext = new StringBuilder();

	    string line;
	    uint in_string = 0;
	    while ((line = text.getline(-1, '\n')) != null)
	    {
		//FIXME: OH GOD am I broken if we're showing binary data
		//with \r\n.
		line = line.TrimEnd(totrim);
		if (!String.IsNullOrEmpty(line))
		{
		    csvtext.Append(line);
		    foreach (char c in line)
			if (c == '"')
			{
			    if (in_string < 2)
				++in_string;
			    else //if (in_string == 2)
				in_string = 1;
			}
			else if (in_string == 2)
			    in_string = 0;
		}
		else if (in_string != 1)
		    break;
 
		csvtext.Append('\n');
		if (in_string != 1)
		{
		    yield return GetOne(csvtext.ToString());
		    csvtext = new StringBuilder();
		}
	    }

	    if (csvtext.Length > 0)
		yield return GetOne(csvtext.ToString());
        }
    }
} //namespace
