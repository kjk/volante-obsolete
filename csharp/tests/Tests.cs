namespace NachoDB
{
    using System;
    using System.Collections;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;

    public class UnitTests
    {
        internal static int totalTests = 0;
        internal static int failedTests = 0;

        public static int TotalTests
        {
            get { return totalTests; }
        }

        public static int FailedTests
        {
            get { return failedTests; }
        }

        public static void AssertThat(bool cond)
        {
            totalTests += 1;
            if (cond) return;
            failedTests += 1;
            // TODO: record callstacks of all failed exceptions
        }

        public static void SafeDeleteFile(string path)
        {
            try
            {
                File.Delete(path);
            }
            catch { }
        }
    }

    public class Record : Persistent
    {
        public string strKey;
        public long intKey;
    }

    public class StringInt : Persistent
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

    public class Test1
    {
        public static void CheckStrings(Index<string, StringInt> root, string[] strs)
        {
            int no = 1;
            foreach (string s in strs)
            {
                StringInt o = root[s];
                UnitTests.AssertThat(o.no == no++);
            }
        }

        public static void Run(bool useAltBtree)
        {
            string dbName = @"testblob.dbs";
            UnitTests.SafeDeleteFile(dbName);
            Storage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = useAltBtree;
            db.Open(dbName);
            Index<string, StringInt> root = (Index<string, StringInt>)db.Root;
            UnitTests.AssertThat(null == root);
            root = db.CreateIndex<string, StringInt>(true);
            db.Root = root;

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
            root = (Index<string, StringInt>)db.Root;
            UnitTests.AssertThat(null != root);
            CheckStrings(root, strs);
            db.Close();
        }
    }

    public class Test2
    {
        public class Root : Persistent
        {
            public Index<string, Record> strIndex;
        }

        public static void Run(bool useAltBtree)
        {
            string dbName = @"testidx.dbs";
            UnitTests.SafeDeleteFile(dbName);

            Storage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = useAltBtree;
            db.Open(dbName);
            Root root = (Root)db.Root;
            UnitTests.AssertThat(null == root);
            root = new Root();
            root.strIndex = db.CreateIndex<string, Record>(true);
            db.Root = root;
            int no = 0;
            string[] strs = new string[] { "one", "two", "three", "four" };
            foreach (string s in strs)
            {
                Record o = new Record();
                o.strKey = s;
                o.intKey = no++;
                root.strIndex[s] = o;
            }
            db.Commit();

            // Test that modyfing an index while traversing it throws an exception
            // Tests AltBtree.BtreeEnumerator
            long n = -1;
            bool gotException = false;
            try
            {
                foreach (Record r in root.strIndex)
                {
                    n = r.intKey;
                    string expectedStr = strs[n];
                    string s = r.strKey;
                    UnitTests.AssertThat(s == expectedStr);

                    if (n == 0)
                    {
                        Record o = new Record();
                        o.strKey = "five";
                        o.intKey = 5;
                        root.strIndex[o.strKey] = o;
                    }
                }
            }
            catch (InvalidOperationException)
            {
                gotException = true;
            }
            UnitTests.AssertThat(gotException);
            UnitTests.AssertThat(n == 0);

            // Test that modyfing an index while traversing it throws an exception
            // Tests AltBtree.BtreeSelectionIterator

            Key keyStart = new Key("four", true);
            Key keyEnd = new Key("three", true);
            gotException = false;
            try
            {
                foreach (Record r in root.strIndex.Range(keyStart, keyEnd, IterationOrder.AscentOrder))
                {
                    n = r.intKey;
                    string expectedStr = strs[n];
                    string s = r.strKey;
                    UnitTests.AssertThat(s == expectedStr);

                    Record o = new Record();
                    o.strKey = "six";
                    o.intKey = 6;
                    root.strIndex[o.strKey] = o;
                }
            }
            catch (InvalidOperationException)
            {
                gotException = true;
            }
            UnitTests.AssertThat(gotException);

            db.Close();
        }

    }

    public class TestXml
    {
        class Record : Persistent
        {
            internal String strKey;
            internal long intKey;
            internal double realKey;
        }

        struct Point
        {
            public int x;
            public int y;
        }

        class Root : Persistent
        {
            internal Index<string, Record> strIndex;
            internal FieldIndex<long, Record> intIndex;
            internal MultiFieldIndex<Record> compoundIndex;
            internal Point      point;
        }

        internal static int pagePoolSize = 32 * 1024 * 1024;

        public static void Run(int nRecords, bool useAltBtree)
        {
            string dbName1 = @"testxml1.dbs";
            string dbName2 = @"testxml2.dbs";
            UnitTests.SafeDeleteFile(dbName1);
            UnitTests.SafeDeleteFile(dbName2);

            string xmlName = useAltBtree ? @"testalt.xml" : @"test.xml";
            Storage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = useAltBtree;
            db.Open(dbName1, pagePoolSize);

            Root root = (Root)db.Root;
            UnitTests.AssertThat(null == root);
            root = new Root();
            root.strIndex = db.CreateIndex<string, Record>(true);
            root.intIndex = db.CreateFieldIndex<long, Record>("intKey", true);
            root.compoundIndex = db.CreateFieldIndex<Record>(new string[]{"strKey", "intKey"}, true);
            root.point.x = 1;
            root.point.y = 2;
            db.Root = root;

            //DateTime start = DateTime.Now;
            Index<string, Record> strIndex = root.strIndex;
            FieldIndex<long, Record> intIndex = root.intIndex;
            MultiFieldIndex<Record> compoundIndex = root.compoundIndex;

            long key = 1999;
            int i;
            for (i = 0; i < nRecords; i++)
            {
                Record rec = new Record();
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                rec.intKey = key;
                rec.strKey = System.Convert.ToString(key);
                rec.realKey = (double)key;
                strIndex.Put(new Key(rec.strKey), rec);
                intIndex.Put(rec);
                compoundIndex.Put(rec);                
            }
            db.Commit();
            //System.Console.WriteLine("Elapsed time for inserting " + nRecords + " records: " + (DateTime.Now - start));

            // start = DateTime.Now
            System.IO.StreamWriter writer = new System.IO.StreamWriter(xmlName);
            db.ExportXML(writer);
            writer.Close();
            db.Close();
            //System.Console.WriteLine("Elapsed time for XML export: " + (DateTime.Now - start));

            //start = DateTime.Now;
            db.Open(dbName2, pagePoolSize);
            System.IO.StreamReader reader = new System.IO.StreamReader(xmlName);
            db.ImportXML(reader);
            reader.Close();
            //System.Console.WriteLine("Elapsed time for XML import: " + (DateTime.Now - start));

            root = (Root)db.Root;
            strIndex = root.strIndex;
            intIndex = root.intIndex;
            compoundIndex = root.compoundIndex;
            UnitTests.AssertThat(root.point.x == 1 && root.point.y == 2);

            //start = DateTime.Now;
            key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                String strKey = System.Convert.ToString(key);
                Record rec1 = strIndex[strKey];
                Record rec2 = intIndex[key];
                Record rec3 = compoundIndex.Get(new Key(strKey, key));
                UnitTests.AssertThat(rec1 != null);
                UnitTests.AssertThat(rec1 == rec2);
                UnitTests.AssertThat(rec1 == rec3);
                UnitTests.AssertThat(rec1.intKey == key);
                UnitTests.AssertThat(rec1.realKey == (double)key);
                UnitTests.AssertThat(strKey.Equals(rec1.strKey));
            }
            db.Close();
            //System.Console.WriteLine("Elapsed time for performing " + nRecords * 2 + " index searches: " + (DateTime.Now - start));
        }
    }

    public class TestBit
    {
        [Flags]
        public enum Options 
        {
            CLASS_A           = 0x00000001,
            CLASS_B           = 0x00000002,
            CLASS_C           = 0x00000004,
            CLASS_D           = 0x00000008,

            UNIVERAL          = 0x00000010,
            SEDAN             = 0x00000020,
            HATCHBACK         = 0x00000040,
            MINIWAN           = 0x00000080,

            AIR_COND          = 0x00000100,
            CLIMANT_CONTROL   = 0x00000200,
            SEAT_HEATING      = 0x00000400,
            MIRROR_HEATING    = 0x00000800,

            ABS               = 0x00001000,
            ESP               = 0x00002000,
            EBD               = 0x00004000,
            TC                = 0x00008000,

            FWD               = 0x00010000,
            REAR_DRIVE        = 0x00020000,
            FRONT_DRIVE       = 0x00040000,

            GPS_NAVIGATION    = 0x00100000,
            CD_RADIO          = 0x00200000,
            CASSETTE_RADIO    = 0x00400000,
            LEATHER           = 0x00800000,

            XEON_LIGHTS       = 0x01000000,
            LOW_PROFILE_TIRES = 0x02000000,
            AUTOMATIC         = 0x04000000,

            DISEL             = 0x10000000,
            TURBO             = 0x20000000,
            GASOLINE          = 0x40000000,
        }

        class Car : Persistent 
        { 
            internal int     hps;
            internal int     maxSpeed;
            internal int     timeTo100;
            internal Options options;
            internal string  model;
            internal string  vendor;
            internal string  specification;
        }

        class Catalogue : Persistent {
            internal FieldIndex<string,Car> modelIndex;
            internal BitIndex<Car>          optionIndex;
        }

        public static void Run(int nRecords)
        {
            int pagePoolSize = 48*1024*1024;
            string dbName = "testbit.dbs";
            
            UnitTests.SafeDeleteFile(dbName);
            Storage db = StorageFactory.CreateStorage();
            db.Open(dbName, pagePoolSize);

            Catalogue root = (Catalogue)db.Root;
            UnitTests.AssertThat(root == null);
            root = new Catalogue();
            root.optionIndex = db.CreateBitIndex<Car>();
            root.modelIndex = db.CreateFieldIndex<string,Car>("model", true);
            db.Root = root;

            BitIndex<Car> index = root.optionIndex;
            DateTime start = DateTime.Now;
            long rnd = 1999;
            int i, n;        

            Options selectedOptions = Options.TURBO|Options.DISEL|Options.FWD|Options.ABS|Options.EBD|Options.ESP|Options.AIR_COND|Options.HATCHBACK|Options.CLASS_C;
            Options unselectedOptions = Options.AUTOMATIC;

            for (i = 0, n = 0; i < nRecords; i++) 
            { 
                rnd = (3141592621L*rnd + 2718281829L) % 1000000007L;
                Options options = (Options)rnd;
                Car car = new Car();
                car.hps = i;
                car.maxSpeed = car.hps * 10;
                car.timeTo100 = 12;
                car.vendor = "Toyota";
                car.specification = "unknown";
                car.model = Convert.ToString(rnd);
                car.options = options;
                root.modelIndex.Put(car);
                root.optionIndex[car] = (int)options;
                if ((options & selectedOptions) == selectedOptions && (options & unselectedOptions) == 0) 
                {
                    n += 1;
                }
            }
            Console.WriteLine("Elapsed time for inserting " + nRecords + " records: " 
                + (DateTime.Now - start));

            start = DateTime.Now;
            i = 0;
            foreach (Car car in root.optionIndex.Select((int)selectedOptions, (int)unselectedOptions)) 
            {
                Debug.Assert((car.options & selectedOptions) == selectedOptions);
                Debug.Assert((car.options & unselectedOptions) == 0);
                i += 1;
            }
            Console.WriteLine("Number of selected cars: " + i);
            Debug.Assert(i == n);
            Console.WriteLine("Elapsed time for bit search through " + nRecords + " records: " 
                + (DateTime.Now - start));

            start = DateTime.Now;
            i = 0;
            foreach (Car car in root.modelIndex) 
            {   
                root.optionIndex.Remove(car);
                car.Deallocate();
                i += 1;
            }
            Debug.Assert(i == nRecords);
            root.optionIndex.Clear();
            Console.WriteLine("Elapsed time for removing " + nRecords + " records: " 
                + (DateTime.Now - start));

            db.Close();
        }
    }

    public class TestBlob 
    {
        public static string FindSrcImplDirectory()
        {
            string dir = Path.Combine("src", "impl");
            if (Directory.Exists(dir))
            {
                return dir;
            }
            dir = Path.Combine("..", dir);
            dir = Path.Combine("..", dir);
            dir = Path.Combine("..", dir);
            dir = Path.Combine("..", dir);
            if (Directory.Exists(dir))
            {
                return dir;
            }
            return null;
        }

        public static string dbName = "testblob.dbs";

        public static void Run()
        {
            Storage db = StorageFactory.CreateStorage();
            db.Open(dbName);
            byte[] buf = new byte[1024];
            int rc;
            string dir = FindSrcImplDirectory();
            string[] files = Directory.GetFiles(dir, "*.cs");
            Index<string,Blob> root = (Index<string,Blob>)db.Root;
            if (root == null) 
            { 
                root = db.CreateIndex<string,Blob>(true);
                db.Root = root;
                foreach (string file in files) 
                { 
                    FileStream fin = new FileStream(file, FileMode.Open, FileAccess.Read);
                    Blob blob = db.CreateBlob();                    
                    Stream bout = blob.GetStream();
                    while ((rc = fin.Read(buf, 0, buf.Length)) > 0) 
                    { 
                        bout.Write(buf, 0, rc);
                    }
                    root[file] = blob; 
                    fin.Close();
                    bout.Close();   
                }
                Console.WriteLine("Database is initialized");
            } 
            foreach (string file in files) 
            {
                byte[] buf2 = new byte[1024];
                Blob blob = root[file];
                if (blob == null) 
                {
                    Console.WriteLine("File " + file + " not found in database");
                    continue;
                }
                Stream bin = blob.GetStream();
                FileStream fin = new FileStream(file, FileMode.Open, FileAccess.Read);
                while ((rc = fin.Read(buf, 0, buf.Length)) > 0) 
                { 
                    int rc2 = bin.Read(buf2, 0, buf2.Length);
                    if (rc != rc2) 
                    {
                        Console.WriteLine("Different file size: " + rc + " .vs. " + rc2);
                        break;
                    }
                    while (--rc >= 0 && buf[rc] == buf2[rc]);
                    if (rc >= 0) 
                    { 
                        Console.WriteLine("Content of the files is different: " + buf[rc] + " .vs. " + buf2[rc]);
                        break;
                    }
                }
                fin.Close();
                bin.Close();
            }            
            Console.WriteLine("Verification completed");
            db.Close();
        }
    }

    public class TestCompoundIndex
    {
        const int pagePoolSize = 32 * 1024 * 1024;

        class Record : Persistent
        {
            internal String strKey;
            internal int intKey;
        }

        public static void Run(bool altBtree, int nRecords)
        {
            string dbName = "testcidx.dbs";
            int i;
            UnitTests.SafeDeleteFile(dbName);
            Storage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = altBtree;
            db.Open(dbName, pagePoolSize);

            MultiFieldIndex<Record> root = (MultiFieldIndex<Record>)db.Root;
            if (root == null)
            {
                root = db.CreateFieldIndex<Record>(new string[] { "intKey", "strKey" }, true);
                db.Root = root;
            }
            DateTime start = DateTime.Now;
            long key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                Record rec = new Record();
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                rec.intKey = (int)((ulong)key >> 32);
                rec.strKey = Convert.ToString((int)key);
                root.Put(rec);
            }
            db.Commit();
            Console.WriteLine("Elapsed time for inserting " + nRecords + " records: " + (DateTime.Now - start));

            start = DateTime.Now;
            key = 1999;
            int minKey = Int32.MaxValue;
            int maxKey = Int32.MinValue;
            for (i = 0; i < nRecords; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                int intKey = (int)((ulong)key >> 32);
                String strKey = Convert.ToString((int)key);
                Record rec = root.Get(new Key(new Object[] { intKey, strKey }));
                Debug.Assert(rec != null && rec.intKey == intKey && rec.strKey.Equals(strKey));
                if (intKey < minKey)
                {
                    minKey = intKey;
                }
                if (intKey > maxKey)
                {
                    maxKey = intKey;
                }
            }
            Console.WriteLine("Elapsed time for performing " + nRecords + " index searches: " + (DateTime.Now - start));

            start = DateTime.Now;
            int n = 0;
            string prevStr = "";
            int prevInt = minKey;
            foreach (Record rec in root.Range(new Key(minKey, ""),
                                              new Key(maxKey + 1, "???"),
                                              IterationOrder.AscentOrder))
            {
                Debug.Assert(rec.intKey > prevInt || rec.intKey == prevInt && rec.strKey.CompareTo(prevStr) > 0);
                prevStr = rec.strKey;
                prevInt = rec.intKey;
                n += 1;
            }
            Debug.Assert(n == nRecords);

            n = 0;
            prevInt = maxKey + 1;
            foreach (Record rec in root.Range(new Key(minKey, "", false),
                                              new Key(maxKey + 1, "???", false),
                                              IterationOrder.DescentOrder))
            {
                Debug.Assert(rec.intKey < prevInt || rec.intKey == prevInt && rec.strKey.CompareTo(prevStr) < 0);
                prevStr = rec.strKey;
                prevInt = rec.intKey;
                n += 1;
            }
            Debug.Assert(n == nRecords);
            Console.WriteLine("Elapsed time for iterating through " + (nRecords * 2) + " records: " + (DateTime.Now - start));
            start = DateTime.Now;
            key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                int intKey = (int)((ulong)key >> 32);
                String strKey = Convert.ToString((int)key);
                Record rec = root.Get(new Key(new Object[] { intKey, strKey }));
                Debug.Assert(rec != null && rec.intKey == intKey && rec.strKey.Equals(strKey));
                Debug.Assert(root.Contains(rec));
                root.Remove(rec);
                rec.Deallocate();
            }
            Debug.Assert(!root.GetEnumerator().MoveNext());
            Debug.Assert(!root.Reverse().GetEnumerator().MoveNext());
            Console.WriteLine("Elapsed time for deleting " + nRecords + " records: " + (DateTime.Now - start));
            db.Close();
        }
    }

    public class TestConcur 
    {
        class L2List : PersistentResource 
        {
            internal L2Elem head;
        }
        
        class L2Elem : Persistent { 
            internal L2Elem next;
            internal L2Elem prev;
            internal int    count;
        
            public override bool RecursiveLoading() { 
                return false;
            }
        
            internal void unlink() { 
                next.prev = prev;
                prev.next = next;
                next.Store();
                prev.Store();
            }
        
            internal void linkAfter(L2Elem elem) {         
                elem.next.prev = this;
                next = elem.next;
                elem.next = this;
                prev = elem;
                Store();
                next.Store();
                prev.Store();
            }
        }

        const int nIterations = 100;
        const int nThreads = 4;
        static int nElements = 0;

        static Storage db;
#if COMPACT_NET_FRAMEWORK
        static int nFinishedThreads;
#endif
        public static void run()
        { 
            L2List list = (L2List)db.Root;
            for (int i = 0; i < nIterations; i++) { 
                long sum = 0, n = 0;
                list.SharedLock();
                L2Elem head = list.head; 
                L2Elem elem = head;
                do { 
                    elem.Load();
                    sum += elem.count;
                    n += 1;
                } while ((elem = elem.next) != head);
                Debug.Assert(n == nElements && sum == (long)nElements*(nElements-1)/2);
                list.Unlock();
                list.ExclusiveLock();
                L2Elem last = list.head.prev;
                last.unlink();
                last.linkAfter(list.head);
                list.Unlock();
            }
#if COMPACT_NET_FRAMEWORK
            lock (typeof(TestConcur)) 
            {
                if (++nFinishedThreads == nThreads) 
                {
                    db.Close();
                }
            }
#endif
        }

        public static void Run(int nEls) 
        {
            string dbName = "testconcur.dbs";
            UnitTests.SafeDeleteFile(dbName);
            TestConcur.nElements = nEls;

            db = StorageFactory.CreateStorage();
            db.Open(dbName);
            L2List list = (L2List)db.Root;
            if (list == null) { 
                list = new L2List();
                list.head = new L2Elem();
                list.head.next = list.head.prev = list.head;
                db.Root = list;
                for (int i = 1; i < nElements; i++) { 
                    L2Elem elem = new L2Elem();
                    elem.count = i;
                    elem.linkAfter(list.head); 
                }
            }
            Thread[] threads = new Thread[nThreads];
            for (int i = 0; i < nThreads; i++) { 
                threads[i] = new Thread(new ThreadStart(run));
                threads[i].Start();
            }
#if !COMPACT_NET_FRAMEWORK
            for (int i = 0; i < nThreads; i++) 
            { 
                threads[i].Join();
            }
#endif
            db.Close();
        }    
    }

    public class TestEnumerator
    { 
        const int pagePoolSize = 32*1024*1024;

        class Record : Persistent 
        { 
            internal String strKey;
            internal long    intKey;
        }

        class Indices : Persistent 
        {
            internal Index<string,Record> strIndex;
            internal Index<long,Record> intIndex;
        }

        static public void Run(int nRecords, bool altBtree)
        {
            string dbName = "testenum.dbs";
            UnitTests.SafeDeleteFile(dbName);

            Storage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = altBtree;
            
            db.Open(dbName, pagePoolSize);
            Indices root = (Indices)db.Root;
            if (root == null) 
            { 
                root = new Indices();
                root.strIndex = db.CreateIndex<string,Record>(false);
                root.intIndex = db.CreateIndex<long,Record>(false);
                db.Root = root;
            }
            Index<long,Record>   intIndex = root.intIndex;
            Index<string,Record> strIndex = root.strIndex;
            Record[] records;
            DateTime start = DateTime.Now;
            long key = 1999;
            int i, j;

            for (i = 0; i < nRecords; i++) 
            { 
                Record rec = new Record();
                key = (3141592621L*key + 2718281829L) % 1000000007L;
                rec.intKey = key;
                rec.strKey = Convert.ToString(key);
                for (j = (int)(key % 10); --j >= 0;) 
                {  
                    intIndex[rec.intKey] = rec;                
                    strIndex[rec.strKey] = rec;        
                }        
            }
            db.Commit();
            Console.WriteLine("Elapsed time for inserting " + nRecords + " records: " + (DateTime.Now - start));
            
            start = DateTime.Now;
            key = 1999;
            for (i = 0; i < nRecords; i++) 
            { 
                key = (3141592621L*key + 2718281829L) % 1000000007L;
                Key fromInclusive = new Key(key);
                Key fromInclusiveStr = new Key(Convert.ToString(key));
                Key fromExclusive = new Key(key, false);
                Key fromExclusiveStr = new Key(Convert.ToString(key), false);
                key = (3141592621L*key + 2718281829L) % 1000000007L;
                Key tillInclusive = new Key(key);
                Key tillInclusiveStr = new Key(Convert.ToString(key));
                Key tillExclusive = new Key(key, false);
                Key tillExclusiveStr = new Key(Convert.ToString(key), false);
                
                // int key ascent order
                records = intIndex.Get(fromInclusive, tillInclusive);
                j = 0;
                foreach (Record rec in intIndex.Range(fromInclusive, tillInclusive, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = intIndex.Get(fromInclusive, tillExclusive);
                j = 0;
                foreach (Record rec in intIndex.Range(fromInclusive, tillExclusive, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = intIndex.Get(fromExclusive, tillInclusive);
                j = 0;
                foreach (Record rec in intIndex.Range(fromExclusive, tillInclusive, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = intIndex.Get(fromExclusive, tillExclusive);
                j = 0;
                foreach (Record rec in intIndex.Range(fromExclusive, tillExclusive, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = intIndex.Get(fromInclusive, null);
                j = 0;
                foreach (Record rec in intIndex.Range(fromInclusive, null, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = intIndex.Get(fromExclusive, null);
                j = 0;
                foreach (Record rec in intIndex.Range(fromExclusive, null, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = intIndex.Get(null, tillInclusive);
                j = 0;
                foreach (Record rec in intIndex.Range(null, tillInclusive, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = intIndex.Get(null, tillExclusive);
                j = 0;
                foreach (Record rec in intIndex.Range(null, tillExclusive, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = intIndex.ToArray();
                j = 0;
                foreach (Record rec in intIndex) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                // int key descent order
                records = intIndex.Get(fromInclusive, tillInclusive);
                j = records.Length;
                foreach (Record rec in intIndex.Range(fromInclusive, tillInclusive, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = intIndex.Get(fromInclusive, tillExclusive);
                j = records.Length;
                foreach (Record rec in intIndex.Range(fromInclusive, tillExclusive, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = intIndex.Get(fromExclusive, tillInclusive);
                j = records.Length;
                foreach (Record rec in intIndex.Range(fromExclusive, tillInclusive, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = intIndex.Get(fromExclusive, tillExclusive);
                j = records.Length;
                foreach (Record rec in intIndex.Range(fromExclusive, tillExclusive, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = intIndex.Get(fromInclusive, null);
                j = records.Length;
                foreach (Record rec in intIndex.Range(fromInclusive, null, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = intIndex.Get(fromExclusive, null);
                j = records.Length;
                foreach (Record rec in intIndex.Range(fromExclusive, null, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = intIndex.Get(null, tillInclusive);
                j = records.Length;
                foreach (Record rec in intIndex.Range(null, tillInclusive, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = intIndex.Get(null, tillExclusive);
                j = records.Length;
                foreach (Record rec in intIndex.Range(null, tillExclusive, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = intIndex.ToArray();
                j = records.Length;
                foreach (Record rec in intIndex.Reverse()) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                // str key ascent order
                records = strIndex.Get(fromInclusiveStr, tillInclusiveStr);
                j = 0;
                foreach (Record rec in strIndex.Range(fromInclusiveStr, tillInclusiveStr, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = strIndex.Get(fromInclusiveStr, tillExclusiveStr);
                j = 0;
                foreach (Record rec in strIndex.Range(fromInclusiveStr, tillExclusiveStr, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = strIndex.Get(fromExclusiveStr, tillInclusiveStr);
                j = 0;
                foreach (Record rec in strIndex.Range(fromExclusiveStr, tillInclusiveStr, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = strIndex.Get(fromExclusiveStr, tillExclusiveStr);
                j = 0;
                foreach (Record rec in strIndex.Range(fromExclusiveStr, tillExclusiveStr, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = strIndex.Get(fromInclusiveStr, null);
                j = 0;
                foreach (Record rec in strIndex.Range(fromInclusiveStr, null, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = strIndex.Get(fromExclusiveStr, null);
                j = 0;
                foreach (Record rec in strIndex.Range(fromExclusiveStr, null, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = strIndex.Get(null, tillInclusiveStr);
                j = 0;
                foreach (Record rec in strIndex.Range(null, tillInclusiveStr, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = strIndex.Get(null, tillExclusiveStr);
                j = 0;
                foreach (Record rec in strIndex.Range(null, tillExclusiveStr, IterationOrder.AscentOrder)) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                records = strIndex.ToArray();
                j = 0;
                foreach (Record rec in strIndex) 
                {
                    Debug.Assert(rec == records[j++]);
                }
                Debug.Assert(j == records.Length);

                // str key descent order
                records = strIndex.Get(fromInclusiveStr, tillInclusiveStr);
                j = records.Length;
                foreach (Record rec in strIndex.Range(fromInclusiveStr, tillInclusiveStr, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = strIndex.Get(fromInclusiveStr, tillExclusiveStr);
                j = records.Length;
                foreach (Record rec in strIndex.Range(fromInclusiveStr, tillExclusiveStr, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = strIndex.Get(fromExclusiveStr, tillInclusiveStr);
                j = records.Length;
                foreach (Record rec in strIndex.Range(fromExclusiveStr, tillInclusiveStr, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = strIndex.Get(fromExclusiveStr, tillExclusiveStr);
                j = records.Length;
                foreach (Record rec in strIndex.Range(fromExclusiveStr, tillExclusiveStr, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = strIndex.Get(fromInclusiveStr, null);
                j = records.Length;
                foreach (Record rec in strIndex.Range(fromInclusiveStr, null, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = strIndex.Get(fromExclusiveStr, null);
                j = records.Length;
                foreach (Record rec in strIndex.Range(fromExclusiveStr, null, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = strIndex.Get(null, tillInclusiveStr);
                j = records.Length;
                foreach (Record rec in strIndex.Range(null, tillInclusiveStr, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = strIndex.Get(null, tillExclusiveStr);
                j = records.Length;
                foreach (Record rec in strIndex.Range(null, tillExclusiveStr, IterationOrder.DescentOrder)) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

                records = strIndex.ToArray();
                j = records.Length;
                foreach (Record rec in strIndex.Reverse()) 
                {
                    Debug.Assert(rec == records[--j]);
                }
                Debug.Assert(j == 0);

               if (i % 100 == 0) { 
                    Console.Write("Iteration " + i + "\n");
                }
            }
            Console.WriteLine("\nElapsed time for performing " + nRecords*36 + " index range searches: "                            + (DateTime.Now - start));

            strIndex.Clear();
            intIndex.Clear();

            Debug.Assert(!strIndex.GetEnumerator().MoveNext());
            Debug.Assert(!intIndex.GetEnumerator().MoveNext());
            Debug.Assert(!strIndex.Reverse().GetEnumerator().MoveNext());
            Debug.Assert(!intIndex.Reverse().GetEnumerator().MoveNext());
            db.Commit();
            db.Gc();
            db.Close();
        }
    }

    public class TestGC
    {
        class PObject : Persistent 
        { 
            internal long    intKey;
            internal PObject next;
            internal String  strKey;
        }

        class StorageRoot : Persistent {
            internal PObject list;
            internal Index<string,PObject> strIndex;
            internal Index<long,PObject>   intIndex;
        }

        const int nObjectsInTree = 10000;

        static public void Run(int nIterations, bool altBtree, bool backgroundGc)
        {
            string dbName = "testgc.dbs";
            UnitTests.SafeDeleteFile(dbName);

            Storage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = altBtree;
            db.BackgroundGc = backgroundGc;
        
            db.Open(dbName);
            db.GcThreshold = 1000000;
            StorageRoot root = new StorageRoot();
            Index<string,PObject> strIndex = root.strIndex = db.CreateIndex<string,PObject>(true);
            Index<long,PObject> intIndex = root.intIndex = db.CreateIndex<long,PObject>(true);
            db.Root = root;
            long insKey = 1999;
            long remKey = 1999;
            
            for (int i = 0; i < nIterations; i++) { 
                if (i > nObjectsInTree) { 
                    remKey = (3141592621L*remKey + 2718281829L) % 1000000007L;
                    intIndex.Remove(new Key(remKey));                
                    strIndex.Remove(new Key(remKey.ToString()));
                }
                PObject obj = new PObject();
                insKey = (3141592621L*insKey + 2718281829L) % 1000000007L;
                obj.intKey = insKey;
                obj.strKey = insKey.ToString();
                obj.next = new PObject();
                intIndex[obj.intKey] = obj;                
                strIndex[obj.strKey] = obj;
                if (i > 0) { 
                    Debug.Assert(root.list.intKey == i-1);
                }
                root.list = new PObject();
                root.list.intKey = i;
                root.Store();
                if (i % 1000 == 0) { 
                    db.Commit();
                    Console.Write("Iteration " + i + "\r");
                }            
            }
            db.Close();
        }
    }

    public class TestIndex
    {
        public class Record : Persistent
        {
            public string strKey;
            public long intKey;
        }

        public class Root : Persistent
        {
            public Index<string,Record> strIndex;
            public Index<long,Record>   intIndex;
        }

        internal static int pagePoolSize = 32 * 1024 * 1024;

        static public void Run(int nRecords, bool altBtree, bool inmemory, bool serializableTransaction)
        {
            int i;
            string dbName = "testidx.dbs";
            UnitTests.SafeDeleteFile(dbName);

            Storage db = StorageFactory.CreateStorage();
            if (altBtree || serializableTransaction)
            {
                db.AlternativeBtree = true;
            }
            if (inmemory)
            {
                pagePoolSize = 0;
            }
            db.Open(dbName, pagePoolSize);

            if (serializableTransaction) 
            { 
                db.BeginThreadTransaction(TransactionMode.Serializable);
            }

            Root root = (Root) db.Root;
            if (root == null)
            {
                root = new Root();
                root.strIndex = db.CreateIndex<string,Record>(true);
                root.intIndex = db.CreateIndex<long,Record>(true);
                db.Root = root;
            }
            Index<string,Record> strIndex = root.strIndex;
            Index<long,Record> intIndex = root.intIndex;
            DateTime start = DateTime.Now;
            long key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                Record rec = new Record();
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                rec.intKey = key;
                rec.strKey = System.Convert.ToString(key);
                intIndex[rec.intKey] = rec;
                strIndex[rec.strKey] = rec;
                if (i % 100000 == 0) 
                { 
                    db.Commit();
                    Console.Write("Iteration " + i + "\r");
                }
            }

            if (serializableTransaction) 
            { 
                db.EndThreadTransaction();
                db.BeginThreadTransaction(TransactionMode.Serializable);
            } 
            else 
            {
                db.Commit();
            }
            System.Console.WriteLine("Elapsed time for inserting " + nRecords + " records: " + (DateTime.Now - start));

            start = System.DateTime.Now;
            key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                Record rec1 = intIndex[key];
                Record rec2 = strIndex[Convert.ToString(key)];
                Debug.Assert(rec1 != null && rec1 == rec2);
            }     
            System.Console.WriteLine("Elapsed time for performing " + nRecords * 2 + " index searches: " + (DateTime.Now - start));

            start = System.DateTime.Now;
            key = Int64.MinValue;
            i = 0;
            foreach (Record rec in intIndex) 
            {
                Debug.Assert(rec.intKey >= key);
                key = rec.intKey;
                i += 1;
            }
            Debug.Assert(i == nRecords);
            i = 0;
            String strKey = "";
            foreach (Record rec in strIndex) 
            {
                Debug.Assert(rec.strKey.CompareTo(strKey) >= 0);
                strKey = rec.strKey;
                i += 1;
            }
            Debug.Assert(i == nRecords);
            System.Console.WriteLine("Elapsed time for iteration through " + (nRecords * 2) + " records: " + (DateTime.Now - start));

            Hashtable map = db.GetMemoryDump();
            Console.WriteLine("Memory usage");
            start = DateTime.Now;
            foreach (MemoryUsage usage in db.GetMemoryDump().Values) 
            { 
                Console.WriteLine(" " + usage.type.Name + ": instances=" + usage.nInstances + ", total size=" + usage.totalSize + ", allocated size=" + usage.allocatedSize);
            }
            Console.WriteLine("Elapsed time for memory dump: " + (DateTime.Now - start));

            start = System.DateTime.Now;
            key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                Record rec = intIndex.Get(key);
                Record removed = intIndex.RemoveKey(key);
                Debug.Assert(removed == rec);
                strIndex.Remove(new Key(System.Convert.ToString(key)), rec);
                rec.Deallocate();
            }
            System.Console.WriteLine("Elapsed time for deleting " + nRecords + " records: " + (DateTime.Now - start));
            db.Close();
        }
    }

    public class TestIndex2
    {
        public class Record : Persistent
        {
            public string strKey;
            public long intKey;
        }

        public class Root : Persistent
        {
            public SortedCollection<string,Record> strIndex;
            public SortedCollection<long,Record>   intIndex;
        }

        public class IntRecordComparator : PersistentComparator<long,Record> 
        {
            public override int CompareMembers(Record m1, Record m2) 
            {
                long diff = m1.intKey - m2.intKey;
                return diff < 0 ? -1 : diff == 0 ? 0 : 1;
            }

            public override int CompareMemberWithKey(Record mbr, long key) 
            {
                long diff = mbr.intKey - key;
                return diff < 0 ? -1 : diff == 0 ? 0 : 1;
            }
        }

        public class StrRecordComparator : PersistentComparator<string,Record> 
        {
            public override int CompareMembers(Record m1, Record m2) 
            {
                return m1.strKey.CompareTo(m2.strKey);
            }

            public override int CompareMemberWithKey(Record mbr, string key) 
            {
                return mbr.strKey.CompareTo(key);
            }
        }

        internal static int pagePoolSize = 0; // infine page pool

        static public void Run(int nRecords)
        {
            string dbName = "testidx2.dbs";
            UnitTests.SafeDeleteFile(dbName);

            int i;
            Storage db = StorageFactory.CreateStorage();
            db.Open(dbName, pagePoolSize);

            Root root = (Root) db.Root;
            UnitTests.AssertThat(root == null);
            root = new Root();
            root.strIndex = db.CreateSortedCollection<string,Record>(new StrRecordComparator(), true);
            root.intIndex = db.CreateSortedCollection<long,Record>(new IntRecordComparator(), true);
            db.Root = root;

            SortedCollection<long,Record> intIndex = root.intIndex;
            SortedCollection<string,Record> strIndex = root.strIndex;
            DateTime start = DateTime.Now;
            long key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                Record rec = new Record();
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                rec.intKey = key;
                rec.strKey = System.Convert.ToString(key);
                intIndex.Add(rec);
                strIndex.Add(rec);
            }
            db.Commit();
            System.Console.WriteLine("Elapsed time for inserting " + nRecords + " records: " + (DateTime.Now - start));
            start = System.DateTime.Now;
            key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                Record rec1 = intIndex[key];
                Record rec2 = strIndex[Convert.ToString(key)];
                Debug.Assert(rec1 != null && rec1 == rec2);
            }     
            System.Console.WriteLine("Elapsed time for performing " + nRecords * 2 + " index searches: " + (DateTime.Now - start));

            start = System.DateTime.Now;
            key = Int64.MinValue;
            i = 0;
            foreach (Record rec in intIndex) 
            {
                Debug.Assert(rec.intKey >= key);
                key = rec.intKey;
                i += 1;
            }
            Debug.Assert(i == nRecords);
            i = 0;
            String strKey = "";
            foreach (Record rec in strIndex) 
            {
                Debug.Assert(rec.strKey.CompareTo(strKey) >= 0);
                strKey = rec.strKey;
                i += 1;
            }
            Debug.Assert(i == nRecords);
            System.Console.WriteLine("Elapsed time for iteration through " + (nRecords * 2) + " records: " + (DateTime.Now - start));

            Hashtable map = db.GetMemoryDump();
            Console.WriteLine("Memory usage");
            start = DateTime.Now;
            foreach (MemoryUsage usage in db.GetMemoryDump().Values) 
            { 
                Console.WriteLine(" " + usage.type.Name + ": instances=" + usage.nInstances + ", total size=" + usage.totalSize + ", allocated size=" + usage.allocatedSize);
            }
            Console.WriteLine("Elapsed time for memory dump: " + (DateTime.Now - start));

            start = System.DateTime.Now;
            key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                Record rec = intIndex[key];
                intIndex.Remove(rec);
                strIndex.Remove(rec);
                rec.Deallocate();
            }
            System.Console.WriteLine("Elapsed time for deleting " + nRecords + " records: " + (DateTime.Now - start));
            db.Close();
        }
    }

}

