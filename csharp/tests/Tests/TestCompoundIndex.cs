namespace Volante
{
    using System;
    using System.Diagnostics;

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
            Tests.SafeDeleteFile(dbName);
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

}
