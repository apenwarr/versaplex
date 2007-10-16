using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Wv.Test
{
    public class WvTest
    {
        public delegate void EmptyCallback();

        protected EmptyCallback inits;
        protected List<KeyValuePair<string,EmptyCallback>> tests
            = new List<KeyValuePair<string,EmptyCallback>>();
        protected EmptyCallback cleanups;

        protected int failures = 0;
        public int Failures {
            get { return failures; }
        }

        public virtual void RegisterTest(string name, EmptyCallback tc)
        {
            tests.Add(new KeyValuePair<string,EmptyCallback>(name, tc));
        }

        public virtual void RegisterInit(EmptyCallback cb)
        {
            inits += cb;
        }

        public virtual void RegisterCleanup(EmptyCallback cb)
        {
            cleanups += cb;
        }

        public virtual void Run()
        {
            System.Console.Out.WriteLine("WvTest: Running all tests");

            foreach (KeyValuePair<string,EmptyCallback> test in tests) {
                System.Console.Out.WriteLine("Testing \"{0}\":", test.Key);

                try {
                    if (inits != null)
                        inits();

                    test.Value();
                } catch (WvAssertionFailure) {
                    failures++;
                } catch (Exception e) {
                    System.Console.Out.WriteLine("! WvTest Exception received FAIL");
                    System.Console.Out.WriteLine(e.ToString());
                    failures++;
                } finally {
                    if (cleanups != null)
                        cleanups();
                }
            }
        }

	public static bool booleanize(bool x)
	{
	    return x;
	}

	public static bool booleanize(long x)
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
	    System.Console.Out.WriteLine("! {0}:{1,-5} {2,-40} {3}",
					 file, line, s,
					 cond ? "ok" : "FAIL");
	    System.Console.Out.Flush();

            if (!cond)
	        throw new WvAssertionFailure(String.Format("{0}:{1} {2}", file, line, s));

	    return cond;
	}

	public static void test_exception(string file, int line, string s)
	{
	    System.Console.Out.WriteLine("! {0}:{1,-5} {2,-40} {3}",
					 file, line, s, "EXCEPTION");
            System.Console.Out.Flush();
	}
	
	public static bool test_eq(long cond1, long cond2,
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

        public WvAssertionFailure(SerializationInfo si, StreamingContext sc)
            : base(si, sc)
        {
        }

        public WvAssertionFailure(string msg, Exception inner)
            : base(msg, inner)
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
    public class SetUpAttribute : Attribute
    {
    }
    public class TearDownAttribute : Attribute
    {
    }
}
