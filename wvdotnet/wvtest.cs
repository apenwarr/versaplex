using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Serialization;

namespace Wv.Test
{
    public class WvTest
    {
	struct TestInfo
	{
	    public string name;
	    public Action cb;
	    
	    public TestInfo(string name, Action cb)
	        { this.name = name; this.cb = cb; }
	}
        List<TestInfo> tests = new List<TestInfo>();

        public int failures { get; private set; }
	
	public WvTest()
	{
	    foreach (Assembly a in AppDomain.CurrentDomain.GetAssemblies())
	    {
		foreach (Type t in a.GetTypes())
		{
		    if (t.IsDefined(typeof(TestFixtureAttribute), false))
		    {
			foreach (MethodInfo m in t.GetMethods())
			{
			    if (m.IsDefined(typeof(TestAttribute), false))
			    {
				// The new t2, m2 are needed so that each
				// delegate gets its own copy of the
				// variable.
				Type t2 = t;
				MethodInfo m2 = m;
				RegisterTest(String.Format("{0}/{1}",
				                t.Name, m.Name),
				     delegate() {
					 try {
					     m2.Invoke(Activator.CreateInstance(t2),
						       null); 
					 } catch (TargetInvocationException e) {
					     throw e.InnerException;
					 }
				     });
			    }
			}
		    }
		}
	    }
	}

        public void RegisterTest(string name, Action tc)
        {
            tests.Add(new TestInfo(name, tc));
        }

	public static void DoMain()
	{
	    // Enough to run an entire test
	    Environment.Exit(new WvTest().Run());
	}

        public int Run()
        {
            Console.WriteLine("WvTest: Running all tests");

            foreach (TestInfo test in tests)
	    {
                Console.WriteLine("\nTesting \"{0}\":", test.name);

                try {
		    test.cb();
                } catch (WvAssertionFailure) {
                    failures++;
                } catch (Exception e) {
                    Console.WriteLine("! WvTest Exception received FAIL");
                    Console.WriteLine(e.ToString());
                    failures++;
                }
            }
	    
	    Console.Out.WriteLine("Result: {0} failures.", failures);
	    
	    // Return a safe unix exit code
	    return failures > 0 ? 1 : 0;
        }

	public static bool booleanize(bool x)
	{
	    return x;
	}

	public static bool booleanize(long x)
	{
	    return x != 0;
	}
	
	public static bool booleanize(ulong x)
	{
	    return x != 0;
	}
	
	public static bool booleanize(string s)
	{
	    return s != null && s != "";
	}
	
	public static bool booleanize(object o)
	{
	    return o != null;
	}
	
	public static bool test(bool cond, string file, int line, string s)
	{
	    Console.WriteLine("! {0}:{1,-5} {2,-40} {3}",
					 file, line, s,
					 cond ? "ok" : "FAIL");
	    Console.Out.Flush();

            if (!cond)
	        throw new WvAssertionFailure(String.Format("{0}:{1} {2}", file, line, s));

	    return cond;
	}

	public static void test_exception(string file, int line, string s)
	{
	    Console.WriteLine("! {0}:{1,-5} {2,-40} {3}",
					 file, line, s, "EXCEPTION");
            Console.Out.Flush();
	}
	
	public static bool test_eq(long cond1, long cond2,
				   string file, int line,
				   string s1, string s2)
	{
	    return test(cond1 == cond2, file, line,
		String.Format("[{0}] == [{1}] ({{{2}}} == {{{3}}})",
			      cond1, cond2, s1, s2));
	}
	
	public static bool test_eq(ulong cond1, ulong cond2,
				   string file, int line,
				   string s1, string s2)
	{
	    return test(cond1 == cond2, file, line,
		String.Format("[{0}] == [{1}] ({{{2}}} == {{{3}}})",
			      cond1, cond2, s1, s2));
	}
	
	public static bool test_eq(double cond1, double cond2,
				   string file, int line,
				   string s1, string s2)
	{
	    return test(cond1 == cond2, file, line,
		String.Format("[{0}] == [{1}] ({{{2}}} == {{{3}}})",
			      cond1, cond2, s1, s2));
	}
	
	public static bool test_eq(decimal cond1, decimal cond2,
				   string file, int line,
				   string s1, string s2)
	{
	    return test(cond1 == cond2, file, line,
		String.Format("[{0}] == [{1}] ({{{2}}} == {{{3}}})",
			      cond1, cond2, s1, s2));
	}
	
	public static bool test_eq(string cond1, string cond2,
				   string file, int line,
				   string s1, string s2)
	{
	    return test(cond1 == cond2, file, line,
		String.Format("[{0}] == [{1}] ({{{2}}} == {{{3}}})",
			      cond1, cond2, s1, s2));
	}

	// some objects can compare themselves to 'null', which is helpful.
	// for example, DateTime.MinValue == null, but only through
	// IComparable, not through IObject.
	public static bool test_eq(IComparable cond1, IComparable cond2,
				   string file, int line,
				   string s1, string s2)
	{
	    return test(cond1.CompareTo(cond2) == 0, file, line,
			String.Format("[{0}] == [{1}]", s1, s2));
	}

	public static bool test_eq(object cond1, object cond2,
				   string file, int line,
				   string s1, string s2)
	{
	    return test(cond1 == cond2, file, line,
		String.Format("[{0}] == [{1}]", s1, s2));
	}

	public static bool test_ne(long cond1, long cond2,
				   string file, int line,
				   string s1, string s2)
	{
	    return test(cond1 != cond2, file, line,
		String.Format("[{0}] != [{1}] ({{{2}}} != {{{3}}})",
			      cond1, cond2, s1, s2));
	}
	
	public static bool test_ne(ulong cond1, ulong cond2,
				   string file, int line,
				   string s1, string s2)
	{
	    return test(cond1 != cond2, file, line,
		String.Format("[{0}] != [{1}] ({{{2}}} != {{{3}}})",
			      cond1, cond2, s1, s2));
	}
	
	public static bool test_ne(double cond1, double cond2,
				   string file, int line,
				   string s1, string s2)
	{
	    return test(cond1 != cond2, file, line,
		String.Format("[{0}] != [{1}] ({{{2}}} != {{{3}}})",
			      cond1, cond2, s1, s2));
	}
	
	public static bool test_ne(decimal cond1, decimal cond2,
				   string file, int line,
				   string s1, string s2)
	{
	    return test(cond1 != cond2, file, line,
		String.Format("[{0}] != [{1}] ({{{2}}} != {{{3}}})",
			      cond1, cond2, s1, s2));
	}
	
	public static bool test_ne(string cond1, string cond2,
				   string file, int line,
				   string s1, string s2)
	{
	    return test(cond1 != cond2, file, line,
		String.Format("[{0}] != [{1}] ({{{2}}} != {{{3}}})",
			      cond1, cond2, s1, s2));
	}
	
	// See notes for test_eq(IComparable,IComparable)
	public static bool test_ne(IComparable cond1, IComparable cond2,
				   string file, int line,
				   string s1, string s2)
	{
	    return test(cond1.CompareTo(cond2) != 0, file, line,
			String.Format("[{0}] != [{1}]", s1, s2));
	}
	
	public static bool test_ne(object cond1, object cond2,
				   string file, int line,
				   string s1, string s2)
	{
	    return test(cond1 != cond2, file, line,
		String.Format("[{0}] != [{1}]", s1, s2));
	}
    }

    public class WvAssertionFailure : Exception
    {
        public WvAssertionFailure()
            : base()
        {
        }

        public WvAssertionFailure(string msg)
            : base(msg)
        {
        }
    }

    // Placeholders for NUnit compatibility
    public class TestFixtureAttribute : Attribute
    {
    }
    public class TestAttribute : Attribute
    {
    }
    [AttributeUsage(AttributeTargets.Method, AllowMultiple=true)]
    public class CategoryAttribute : Attribute
    {
        public CategoryAttribute(string x)
        {
        }
    }
}
