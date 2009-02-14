using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Wv;
using Wv.Extensions;

namespace Wv.Query
{
    // API (currently very loosely) based on:
    // http://docs.djangoproject.com/en/dev/topics/db/queries/
     
    class ColumnAttribute : Attribute { }
    class PrimaryKeyColumnAttribute : ColumnAttribute { }
     
    internal abstract class Queryable
    {
	abstract internal void _fill(WvSqlRow r);
    }
    
    class QuerySet<T> : IEnumerable<T> where T: Queryable, new()
    {
	public struct WhereClause
	{
	    public WhereClause(string name, string op, string value)
	        { this.name = name; this.op = op; this.value = value; }
	    public string name, op, value;
	}
	
	WvDbi dbi;
	List<WhereClause> wheres;
	
	QuerySet(WvDbi dbi, List<WhereClause> wheres)
	{
	    this.dbi = dbi;
	    this.wheres = wheres;
	}
	
	public QuerySet(WvDbi dbi) 
	    : this(dbi, new List<WhereClause>())
	    { }
	
	public QuerySet<T> filter(string name, string op, string value)
	{
	    var wl = new List<WhereClause>(wheres);
	    wl.Add(new WhereClause(name, op, value));
	    return new QuerySet<T>(dbi, wl);
	}
	
	public QuerySet<T> filter(string name, string value)
	{
	    return filter(name, "=", value);
	}
	
	public QuerySet<T> exclude(string name, string value)
	{
	    return filter(name, "<>", value);
	}
	
	IEnumerable<T> runq()
	{
	    string[] m = 
		(from i
		in WvReflection.find_members(typeof(T), typeof(ColumnAttribute))
		select i.Name).ToArray();
	    
	    string q = wv.fmt("select {0} from {1}",
			      m.join(", "), typeof(T).Name);
	    
	    var names = from w in wheres select (w.name + "=?");
	    var values = from w in wheres select w.value;
	    
	    if (wheres.Count > 0) 
		q += " where " + names.join(" and ");
	    
	    wv.printerr("QUERY: '{0}' ({1})\n", q, values.join("; "));
	    
	    foreach (var r in dbi.select(q, values.ToArray()))
	    {
		T t = new T();
		t._fill(r);
		yield return t;
	    }
	}
	
	IEnumerator System.Collections.IEnumerable.GetEnumerator()
	{
	    foreach (var r in runq())
		yield return r;
	}
	
	public IEnumerator<T> GetEnumerator()
	{
	    foreach (var r in runq())
		yield return r;
	}
    }
     
    class MyDb
    {
	WvDbi dbi;
	
	public MyDb(WvDbi dbi)
	{
	    this.dbi = dbi;
	}
	
	public QuerySet<Testy> Testy()
	    { return new QuerySet<Testy>(dbi); }
    }
    
    class Testy : Queryable
    {
	[PrimaryKeyColumn] public int id;
	[Column] public string name;
	[Column] public int age;
	[Column] public DateTime birthdate;
	
	public Testy() { }
        internal override void _fill(WvSqlRow _r)
	{
	    var _e = _r.GetEnumerator();
	    id = _e.pop();
	    name = _e.pop();
	    age = _e.pop();
	    birthdate = _e.pop();
	}
    }
    
    public static class WrapTest
    {
	public static void Main()
	{
	    wv.print("Hello, world\n");
	    
	    WvDbi dbi = WvDbi.create("sqlite:foo.db");
	    var db = new MyDb(dbi);
	    
	    foreach (Testy t in db.Testy().filter("name", "Jana"))
		wv.print("X: '{0};{1};{2};{3}'\n", 
			 t.id, t.name, t.age, t.birthdate);
	}
    }
}
