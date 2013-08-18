// Copyright: Krzysztof Kowalczyk
// License: BSD

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;
using Volante;

public interface ITest
{
    void Run(TestConfig config);
}

public struct SimpleStruct
{
    public int v1;
    public long v2;
}

public enum RecordFullEnum
{
    Zero = 0,
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
    Five = 5,
    Six = 6,
    Seven = 7,
    Eight = 8,
    Nine = 9,
    Ten = 10
}

public class TestFileListener : FileListener
{
    int WriteCount = 0;
    int ReadCount = 0;
    int SyncCount = 0;

    public override void OnWrite(long pos, long len)
    {
        base.OnWrite(pos, len);
        WriteCount++;
    }

    public override void OnRead(long pos, long bufSize, long read)
    {
        base.OnRead(pos, bufSize, read);
        ReadCount++;
    }

    public override void OnSync()
    {
        base.OnSync();
        SyncCount++;
    }
}

public class TestDatabaseListener : DatabaseListener
{
    int DatabaseCorruptedCount;
    int RecoveryCompletedCount;
    int GcStartedCount;
    int GcCompletedCount;
    int DeallocateObjectCount;
    int ReplicationErrorCount;

    public override void DatabaseCorrupted()
    {
        base.DatabaseCorrupted();
        DatabaseCorruptedCount++;
    }

    public override void RecoveryCompleted()
    {
        base.RecoveryCompleted();
        RecoveryCompletedCount++;
    }

    public override void GcStarted()
    {
        base.GcStarted();
        GcStartedCount++;
    }

    public override void GcCompleted(int nDeallocatedObjects)
    {
        base.GcCompleted(nDeallocatedObjects);
        GcCompletedCount++;
    }

    public override void DeallocateObject(Type cls, int oid)
    {
        base.DeallocateObject(cls, oid);
        DeallocateObjectCount++;
    }

    public override bool ReplicationError(string host)
    {
        ReplicationErrorCount++;
        return base.ReplicationError(host);
    }
}

// Note: this object should allow generating dynamic code
// for serialization/deserialization. Among other things,
// it cannot contain properties (because they are implemented
// as private backing fields), enums. The code that decides
// what can be generated like that is ClassDescriptor.generateSerializer()
public class RecordFull : Persistent
{
    public Boolean BoolVal;
    public byte ByteVal;
    public sbyte SByteVal;
    public Int16 Int16Val;
    public UInt16 UInt16Val;
    public Int32 Int32Val;
    public UInt32 UInt32Val;
    public Int64 Int64Val;
    public UInt64 UInt64Val;
    public char CharVal;
    public float FloatVal;
    public double DoubleVal;
    public DateTime DateTimeVal;
    public Decimal DecimalVal;
    public Guid GuidVal;
    public string StrVal;

    public RecordFullEnum EnumVal;
    public object ObjectVal;

    public RecordFull()
    {
    }

    public virtual void SetValue(Int64 v)
    {
        BoolVal = (v % 2 == 0) ? false : true;
        ByteVal = (byte)v;
        SByteVal = (sbyte)v;
        Int16Val = (Int16)v;
        UInt16Val = (UInt16)v;
        Int32Val = (Int32)v;
        UInt32Val = (UInt32)v;
        Int64Val = v;
        UInt64Val = (UInt64)v;
        CharVal = (char)v;
        FloatVal = (float)v;
        DoubleVal = Convert.ToDouble(v);
        DateTimeVal = DateTime.Now;
        DecimalVal = Convert.ToDecimal(v);
        GuidVal = Guid.NewGuid();
        StrVal = v.ToString();

        int enumVal = (int)(v % 11);
        EnumVal = (RecordFullEnum)enumVal;
        ObjectVal = (object)v;
    }

    public RecordFull(Int64 v)
    {
        SetValue(v);
    }
}

// used for FieldIndex
public class RecordFullWithProperty : RecordFull
{
    public Int64 Int64Prop { get; set; }

    public override void SetValue(Int64 v)
    {
        base.SetValue(v);
        Int64Prop = v;
    }

    public RecordFullWithProperty(Int64 v)
    {
        SetValue(v);
    }

    public RecordFullWithProperty()
    {
    }
}

public class TestConfig
{
    const int INFINITE_PAGE_POOL = 0;

    public enum InMemoryType
    {
        // uses a file
        No,
        // uses NullFile and infinite page pool
        Full,
        // uses a real file and infinite page pool. The work is done
        // in memory but on closing dta is persisted to a file
        File
    };

    public enum FileType
    {
        File, // use IFile
        Stream // use StreamFile
    };

    public string TestName;
    public InMemoryType InMemory = InMemoryType.No;
    public FileType FileKind = FileType.File;
    public bool AltBtree = false;
    public bool Serializable = false;
    public bool BackgroundGc = false;
    public bool CodeGeneration = true;
    public bool Encrypted = false;
    public bool IsTransient = false;
    public CacheType CacheKind = CacheType.Lru;
    public int Count = 0; // number of iterations

    // Set by the test. Can be a subclass of TestResult
    public TestResult Result;

    public string DatabaseName
    {
        get
        {
            string p1 = AltBtree ? "_alt" : "";
            string p2 = Serializable ? "_ser" : "";
            string p3 = (InMemory == InMemoryType.Full) ? "_mem" : "";
            string p4 = "";
            if (InMemory != InMemoryType.Full)
                p4 = (FileKind == FileType.File) ? "_file" : "_stream";
            string p5 = String.Format("_{0}", Count);
            string p6 = CodeGeneration ? "_cg" : "";
            string p7 = Encrypted ? "_enc" : "";
            string p8 = (CacheKind != CacheType.Lru) ? CacheKind.ToString() : "";
            return String.Format("{0}{1}{2}{3}{4}{5}{6}{7}{8}.dbs", TestName, p1, p2, p3, p4, p5, p6, p7, p8);
        }
    }

    void OpenTransientDatabase(IDatabase db)
    {
        NullFile dbFile = new NullFile();
        dbFile.Listener = new TestFileListener();
        Tests.Assert(dbFile.NoFlush == false);
        dbFile.NoFlush = true;
        Tests.Assert(dbFile.NoFlush == false);
        Tests.Assert(dbFile.Length == 0);
        db.Open(dbFile, INFINITE_PAGE_POOL);
        IsTransient = true;
    }

    public IDatabase GetDatabase(bool delete=true)
    {
        IDatabase db = DatabaseFactory.CreateDatabase();
        Tests.Assert(db.CodeGeneration);
        Tests.Assert(!db.BackgroundGc);
        db.Listener = new TestDatabaseListener();
#if WITH_OLD_BTREE
        db.AlternativeBtree = AltBtree || Serializable;
#endif
        db.BackgroundGc = BackgroundGc;
        db.CacheKind = CacheKind;
        db.CodeGeneration = CodeGeneration;
        // TODO: make it configurable?
        // TODO: make it bigger (1000000 - the original value for h)
        if (BackgroundGc)
            db.GcThreshold = 100000;
        if (InMemory == InMemoryType.Full)
            OpenTransientDatabase(db);
        else
        {
            var name = DatabaseName;
            if (delete)
                Tests.TryDeleteFile(name);
            if (InMemory == InMemoryType.File)
            {
                if (FileKind == FileType.File)
                    db.Open(name, INFINITE_PAGE_POOL);
                else
                {
                    var f = File.Open(name, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
                    var sf = new StreamFile(f);
                    db.Open(sf, INFINITE_PAGE_POOL);
                }
                db.File.Listener = new TestFileListener();
            }
            else
            {
                if (FileKind == FileType.File)
                {
                    if (Encrypted)
                        db.Open(new Rc4File(name, "PassWord"));
                    else
                        db.Open(name);
                }
                else
                {
                    FileMode m = FileMode.CreateNew;
                    if (!delete)
                        m = FileMode.OpenOrCreate;
                    var f = File.Open(name, m, FileAccess.ReadWrite, FileShare.None);
                    var sf = new StreamFile(f);
                    db.Open(sf);
                }
                db.File.Listener = new TestFileListener();
            }
        }
        return db;
    }

    public TestConfig()
    {
    }

    public TestConfig Clone()
    {
        return (TestConfig)MemberwiseClone();
    }
}

public class TestResult
{
    public bool Ok;
    public TestConfig Config;
    public string TestName; // TODO: get rid of it after converting all tests to TestConfig
    public int Count;
    public TimeSpan ExecutionTime;

    public override string ToString()
    {
        string name = TestName;
        if (null != Config)
            name = Config.DatabaseName;
        if (Ok)
            return String.Format("OK, {0,6} ms {1}", (int)ExecutionTime.TotalMilliseconds, name);
        else
            return String.Format("FAILED {0}", name);
    }

    public void Print()
    {
        System.Console.WriteLine(ToString());
    }
}

public class TestIndexNumericResult : TestResult
{
    public TimeSpan InsertTime;
}

public class Tests
{
    internal static int TotalTests = 0;
    internal static int FailedTests = 0;
    internal static int CurrAssertsCount = 0;
    internal static int CurrAssertsFailed = 0;

    internal static List<StackTrace> FailedStackTraces = new List<StackTrace>();
    static void ResetAssertsCount()
    {
        CurrAssertsCount = 0;
        CurrAssertsFailed = 0;
    }

    public static int DbInstanceCount(IDatabase db, Type type)
    {
        var mem = db.GetMemoryUsage();
        TypeMemoryUsage mu;
        bool ok = mem.TryGetValue(type, out mu);
        if (!ok)
            return 0;
        return mu.Count;
    }

    public static void DumpMemoryUsage(ICollection<TypeMemoryUsage> usages)
    {
        Console.WriteLine("Memory usage");
        foreach (TypeMemoryUsage usage in usages)
        {
            Console.WriteLine(" " + usage.Type.Name + ": instances=" + usage.Count + ", total size=" + usage.TotalSize + ", allocated size=" + usage.AllocatedSize);
        }
    }

    public static void Assert(bool cond)
    {
        CurrAssertsCount += 1;
        if (cond) return;
        CurrAssertsFailed += 1;
        FailedStackTraces.Add(new StackTrace());
    }

    public delegate void Action();
    public static void AssertException<TExc>(Action func)
        where TExc : Exception
    {
        bool gotException = false;
        try
        {
            func();
        }
        catch (TExc)
        {
            gotException = true;
        }
        Assert(gotException);
    }

    public static bool ByteArraysEqual(byte[] a1, byte[] a2)
    {
        if (a1 == a2)
            return true;
        if (a1 == null || a2 == null)
            return false;
        if (a1.Length != a2.Length)
            return false;
        for (var i = 0; i < a1.Length; i++)
        {
            if (a1[i] != a2[i])
                return false;
        }
        return true;
    }
 
    public static void AssertDatabaseException(Action func, DatabaseException.ErrorCode expectedCode)
    {
        bool gotException = false;
        try
        {
            func();
        }
        catch (DatabaseException exc)
        {
            gotException = exc.Code == expectedCode;
        }
        Assert(gotException);
    }

    public static IEnumerable<long> KeySeq(int count)
    {
        long v = 1999;
        for (int i = 0; i < count; i++)
        {
            yield return v;
            v = (3141592621L * v + 2718281829L) % 1000000007L;
        }
    }

    public static bool FinalizeTest()
    {
        TotalTests += 1;
        if (CurrAssertsFailed > 0)
            FailedTests += 1;
        bool ok = CurrAssertsFailed == 0;
        ResetAssertsCount();
        return ok;
    }

    public static void PrintFailedStackTraces()
    {
        int max = 5;
        foreach (var st in FailedStackTraces)
        {
            Console.WriteLine(st.ToString() + "\n");
            if (--max == 0)
                break;
        }
    }

    public static bool TryDeleteFile(string path)
    {
        try
        {
            File.Delete(path);
            return true;
        }
        catch 
        {
            return false;
        }
    }

    public static void VerifyDictionaryEnumeratorDone(IDictionaryEnumerator de)
    {
        AssertException<InvalidOperationException>(
            () => { Console.WriteLine(de.Current); });
        AssertException<InvalidOperationException>(
            () => { Console.WriteLine(de.Entry); });
        AssertException<InvalidOperationException>(
            () => { Console.WriteLine(de.Key); });
        AssertException<InvalidOperationException>(
            () => { Console.WriteLine(de.Value); });
        Tests.Assert(!de.MoveNext());
    }

    public static void VerifyEnumeratorDone(IEnumerator e)
    {
        AssertException<InvalidOperationException>(
            () => { Console.WriteLine(e.Current); });
        Tests.Assert(!e.MoveNext());
    }
}

public class TestsMain
{
    const int CountsIdxFast = 0;
    const int CountsIdxSlow = 1;
    static int CountsIdx = CountsIdxFast;
    static int[] CountsDefault = new int[2] { 1000, 100000 };
    static int[] Counts1 = new int[2] { 200, 10000 };

    static TestConfig[] ConfigsDefault = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full },
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full, AltBtree=true }
    };

    static TestConfig[] ConfigsR2 = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full },
        // TODO: should have a separate NoFlush flag
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full, AltBtree=true }
    };

    static TestConfig[] ConfigsRaw = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full },
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full, AltBtree=true }
    };

    static TestConfig[] ConfigsNoAlt = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full }
    };

    static TestConfig[] ConfigsOnlyAlt = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full, AltBtree=true }
    };

    static TestConfig[] ConfigsOneFileAlt = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.File, AltBtree=true }
    };

    static TestConfig[] ConfigsIndex = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, AltBtree=true, CacheKind = CacheType.Weak, Count = 2500 },
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full },
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full, Serializable=true },
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, AltBtree=false },
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, AltBtree=true },
        new TestConfig{ InMemory = TestConfig.InMemoryType.Full, AltBtree=true },
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, AltBtree=true, CodeGeneration=false },
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, AltBtree=true, Encrypted=true },
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, FileKind = TestConfig.FileType.Stream, AltBtree=true }
    };

    static TestConfig[] ConfigsDefaultFile = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.No },
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, AltBtree=true }
    };

    static TestConfig[] ConfigsGc = new TestConfig[] {
        new TestConfig{ InMemory = TestConfig.InMemoryType.No },
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, AltBtree=true },
        new TestConfig{ InMemory = TestConfig.InMemoryType.No, AltBtree=true, BackgroundGc = true }
    };

    public class TestInfo
    {
        public string Name;
        public TestConfig[] Configs;
        public int[] Counts;
        public int Count { get { return Counts[CountsIdx]; } }

        public TestInfo(string name, TestConfig[] configs = null, int[] counts=null)
        {
            Name = name;
            if (null == configs)
                configs = ConfigsDefault;
            Configs = configs;
            if (null == counts)
                counts = CountsDefault;
            Counts = counts;
        }
    };

    static TestInfo[] TestInfos = new TestInfo[]
    {
        // small count for TestFieldIndex and TestMultiFieldIndex because we only
        // want to test code paths unique to them. The underlying code is tested
        // in regular index tests
        new TestInfo("TestFieldIndex", ConfigsDefault, new int[2] { 100, 100 }),
        new TestInfo("TestMultiFieldIndex", ConfigsDefault, new int[2] { 100, 100 }),
        new TestInfo("TestR2", ConfigsR2, new int[2] { 1500, 20000 }),
        new TestInfo("TestRtree", ConfigsDefault, new int[2] { 1500, 20000 }),
        new TestInfo("TestCorrupt01", ConfigsOneFileAlt, Counts1),
        new TestInfo("TestIndex", ConfigsIndex, Counts1),
        new TestInfo("TestProjection"),
        new TestInfo("TestL2List", ConfigsDefault, new int[2] { 500, 500 }),
        new TestInfo("TestTtree", ConfigsDefault, new int[2] { 10020, 100000 }),
        new TestInfo("TestTimeSeries", ConfigsDefault, new int[2] { 10005, 100005 }),
        new TestInfo("TestThickIndex"),
#if WITH_PATRICIA
        new TestInfo("TestPatriciaTrie"),
#endif
        new TestInfo("TestLinkPArray"),
        // test set below ScalableSet.BTREE_THRESHOLD, which is 128, to test
        // ILink code paths
        new TestInfo("TestSet", ConfigsOnlyAlt, new int[2] { 100, 100 }),
        new TestInfo("TestSet", ConfigsDefault, CountsDefault),
#if WITH_REPLICATION
        new TestInfo("TestReplication", ConfigsOnlyAlt, new int[2] { 10000, 500000 }),
#endif
        new TestInfo("TestIndex", ConfigsIndex, Counts1),
#if WITH_XML
        new TestInfo("TestXml", ConfigsDefaultFile, new int[2] { 2000, 20000 }),
#endif
        new TestInfo("TestIndexRangeSearch"),
        new TestInfo("TestCorrupt00", ConfigsOneFileAlt),
        new TestInfo("TestRemove00"),
        new TestInfo("TestIndexUInt00"),
        new TestInfo("TestIndexInt00"),
        new TestInfo("TestIndexInt"),
        new TestInfo("TestIndexUInt"),
        new TestInfo("TestIndexBoolean"),
        new TestInfo("TestIndexByte"),
        new TestInfo("TestIndexSByte"),
        new TestInfo("TestIndexShort"),
        new TestInfo("TestIndexUShort"),
        new TestInfo("TestIndexLong"),
        new TestInfo("TestIndexULong"),
        new TestInfo("TestIndexDecimal"),
        new TestInfo("TestIndexFloat"),
        new TestInfo("TestIndexDouble"),
        new TestInfo("TestIndexGuid"),
        new TestInfo("TestIndexObject"),
        new TestInfo("TestIndexDateTime", ConfigsDefaultFile),
        new TestInfo("TestIndex2"),
        new TestInfo("TestIndex3"),
        new TestInfo("TestIndex4", ConfigsDefaultFile, Counts1),
#if WITH_OLD_BTREE
        new TestInfo("TestBit", ConfigsNoAlt, new int[2] { 2000, 20000 }),
#endif
        new TestInfo("TestRaw", ConfigsRaw, new int[2] { 1000, 10000 }),
        new TestInfo("TestBlob", ConfigsOneFileAlt),
        new TestInfo("TestConcur"),
        new TestInfo("TestEnumerator", ConfigsDefault, new int[2] { 50, 1000 }),
        // TODO: figure out why running it twice throws an exception from reflection
        // about trying to create a duplicate wrapper class
        new TestInfo("TestList", ConfigsOnlyAlt),
        // TODO: figure out why when it's 2000 instead of 2001 we fail
        new TestInfo("TestBackup", ConfigsDefaultFile),
        new TestInfo("TestGc", ConfigsGc, new int[2] { 5000, 50000 })
    };

    static void ParseCmdLineArgs(string[] args)
    {
        foreach (var arg in args)
        {
            if (arg == "-fast")
                CountsIdx = CountsIdxFast;
            else if (arg == "-slow")
                CountsIdx = CountsIdxSlow;
        }
    }

    public static void RunTests(TestInfo testInfo)
    {
        string testClassName = testInfo.Name;
        var assembly = Assembly.GetExecutingAssembly();
        object obj = assembly.CreateInstance(testClassName);
        if (obj == null)
            obj = assembly.CreateInstance("Volante." + testClassName);
        ITest test = (ITest)obj;
        foreach (TestConfig configTmp in testInfo.Configs)
        {
#if !WITH_OLD_BTREE
            bool useAltBtree = configTmp.AltBtree || configTmp.Serializable;
            if (!useAltBtree)
                continue;
#endif
        var config = configTmp.Clone();
        if (configTmp.Count != 0)
            config.Count = configTmp.Count;
        else
            config.Count = testInfo.Count;
        config.TestName = testClassName;
        config.Result = new TestResult(); // can be over-written by a test
        DateTime start = DateTime.Now;
        test.Run(config);
        config.Result.ExecutionTime = DateTime.Now - start;
        config.Result.Config = config; // so that we Print() nicely
        config.Result.Ok = Tests.FinalizeTest();
        config.Result.Print();
        }
    }

    public static void Main(string[] args)
    {
        ParseCmdLineArgs(args);

        var tStart = DateTime.Now;

        foreach (var t in TestInfos)
        {
            RunTests(t);
        }

        var tEnd = DateTime.Now;
        var executionTime = tEnd - tStart;

        if (0 == Tests.FailedTests)
        {
            Console.WriteLine(String.Format("OK! All {0} tests passed", Tests.TotalTests));
        }
        else
        {
            Console.WriteLine(String.Format("FAIL! Failed {0} out of {1} tests", Tests.FailedTests, Tests.TotalTests));
        }
        Tests.PrintFailedStackTraces();
        Console.WriteLine(String.Format("Running time: {0} ms", (int)executionTime.TotalMilliseconds));
    }
}
