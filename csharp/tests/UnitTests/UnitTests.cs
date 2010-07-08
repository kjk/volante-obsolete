using System;
using NachoDB;
using System.IO;

public class UnitTests
{
    internal static int totalTests = 0;
    internal static int failedTests = 0;

    public static int TotalTests {
        get { return totalTests; }
    }

    public static int FailedTests {
        get { return failedTests; }
    }

    public static void AssertThat(bool cond)
    {
        totalTests += 1;
        if (cond) return;
        failedTests += 1;
        // TODO: add some logging
    }

    public static void SafeDeleteFile(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch {}
    }
}

public class UnitTest1
{
    class StringInt : Persistent
    {
        public string s;
        public int no;
        public StringInt()
        {
        }
        public StringInt(string s, int no)
        {
            this.s = s;
            this.no = no;
        }
    }

#if USE_GENERICS
    public static void CheckStrings(Index<string,int> root, string[] strs)
#else
    public static void CheckStrings(Index root, string[] strs)
#endif
    {
        int no = 1;
        foreach (string s in strs)
        {
#if USE_GENERICS
            StringInt o = root[s];
#else
            StringInt o = (StringInt)root[s];
#endif
            UnitTests.AssertThat(o.no == no++);
        }
    }

    public static void Run()
    {
        string dbName = @"testblob.dbs";
        UnitTests.SafeDeleteFile(dbName);
        Storage db = StorageFactory.CreateStorage();
        db.Open(dbName);
#if USE_GENERICS
        Index<string,int> root = (Index<string,StringInt>)db.Root;
#else
        Index root = (Index)db.Root;
#endif
        UnitTests.AssertThat(null == root);
#if USE_GENERICS
        root = db.CreateIndex<string,StringInt>(true);
#else
        root = db.CreateIndex(typeof(string), true);
#endif
        db.Root = root;
        db.Commit();

        int no = 1;
        string[] strs = new string[] { "one", "two", "three", "four" };
        foreach (string s in strs) 
        { 
            var o = new StringInt(s, no++);
            root[s] = o;
        }

        CheckStrings(root, strs);
        db.Close();

        db = StorageFactory.CreateStorage();
        db.Open(dbName);
#if USE_GENERICS
        root = (Index<string,StringInt>)db.Root;
#else
        root = (Index)db.Root;
#endif
        UnitTests.AssertThat(null != root);
        CheckStrings(root, strs);
        db.Close();
    }
}

public class UnitTestsRunner
{ 
    public static void Main(string[] args) 
    {
        UnitTest1.Run();
        Console.WriteLine(String.Format("Failed {0} out of {1} tests", UnitTests.FailedTests, UnitTests.TotalTests));
    }
}

