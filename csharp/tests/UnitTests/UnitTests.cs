namespace NachoDB
{
    using System;
    using System.Diagnostics;
    using System.IO;

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

    public class UnitTest1
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

    public class UnitTest2
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

    public class UnitTestXml
    {
        class Record : Persistent
        {
            internal String strKey;
            internal long intKey;
            internal double realKey;
        }

        class Root : Persistent
        {
            internal Index<string, Record> strIndex;
            internal FieldIndex<long, Record> intIndex;
        }

        internal static int pagePoolSize = 32 * 1024 * 1024;

        public static void Run(int nRecords, bool useAltBtree)
        {
            string dbName = @"testxml.dbs";
            string xmlName = useAltBtree ? @"testalt.xml" : @"test.xml";
            UnitTests.SafeDeleteFile(dbName);
            Storage db = StorageFactory.CreateStorage();
            db.AlternativeBtree = useAltBtree;
            db.Open(dbName, pagePoolSize);
            Root root = (Root)db.Root;
            UnitTests.AssertThat(null == root);
            root = new Root();
            root.strIndex = db.CreateIndex<string, Record>(true);
            root.intIndex = db.CreateFieldIndex<long, Record>("intKey", true);
            db.Root = root;
            DateTime start = DateTime.Now;
            long key = 1999;
            Index<string, Record> strIndex = root.strIndex;
            FieldIndex<long, Record> intIndex = root.intIndex;
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
            }
            db.Commit();

            System.IO.StreamWriter writer = new System.IO.StreamWriter(xmlName);
            db.ExportXML(writer);
            writer.Close();
            db.Close();

            UnitTests.SafeDeleteFile(dbName);
            db.Open(dbName, pagePoolSize);
            System.IO.StreamReader reader = new System.IO.StreamReader(xmlName);
            db.ImportXML(reader);
            reader.Close();

            root = (Root)db.Root;
            strIndex = root.strIndex;
            intIndex = root.intIndex;

            key = 1999;
            for (i = 0; i < nRecords; i++)
            {
                key = (3141592621L * key + 2718281829L) % 1000000007L;
                String strKey = System.Convert.ToString(key);
                Record rec1 = strIndex[strKey];
                Record rec2 = intIndex[key];
                //Record rec3 = compoundIndex.Get(new Key(strKey, key));
                UnitTests.AssertThat(rec1 != null);
                UnitTests.AssertThat(rec1 == rec2);
                UnitTests.AssertThat(rec1.intKey == key);
                UnitTests.AssertThat(rec1.realKey == (double)key);
                UnitTests.AssertThat(strKey.Equals(rec1.strKey));
                /*
                Debug.Assert(rec1 == rec3);
                */
            }
            db.Close();
        }
    }

    public class UnitTestBit
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

    public class UnitTestBlob 
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
}

