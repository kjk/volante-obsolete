using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Volante;

namespace DirectoryScan
{
    class FileEntry : Persistent
    {
        public string Path;
        public long Size;
        public DateTime CreationTimeUtc;
        public DateTime LastAccessTimeUtc;
        public DateTime LastWriteTimeUtc;
    }

    class DatabaseRoot : Persistent
    {
        public Index<long, FileEntry> FileSizeIndex;
        public Index<string, FileEntry> FileNameIndex;
        public Index<DateTime, FileEntry> FileLastWriteTimeIndex;
    }

    class DirectoryScan
    {
        const int LIMIT = 5;

        static void Main(string[] args)
        {
            IStorage db = StorageFactory.CreateStorage();
            string dbName = "fileinfo.dbs";
            db.Open(dbName);
            DatabaseRoot dbRoot = null;
            if (null != db.Root)
            {
                dbRoot = (DatabaseRoot)db.Root;
            }
            else
            {
                // only create root once
                dbRoot = new DatabaseRoot(); 
                dbRoot.FileSizeIndex = db.CreateIndex<Int64, FileEntry>(false);
                dbRoot.FileNameIndex = db.CreateIndex<string, FileEntry>(false);
                dbRoot.FileLastWriteTimeIndex = db.CreateIndex<DateTime, FileEntry>(false);
                db.Root = dbRoot;
                // changing the root marks database as modified but it's
                // only modified in memory. Commit to persist changes to disk.
                db.Commit();
                PopulateDatabase(db, "c:\\");
            }

            ListSmallestFiles(dbRoot, LIMIT);
            ListBiggestFiles(dbRoot, LIMIT);
            ListMostRecentlyWrittenToFiles(dbRoot, LIMIT);
            ListDuplicateNamesFiles(dbRoot, LIMIT);
            db.Close();
        }

        static void PopulateDatabase(IStorage db, string startDir)
        {
            DatabaseRoot dbRoot = (DatabaseRoot)db.Root;
            // scan all directories starting with StartDir
            var dirsToVisit = new List<string>() { StartDir };
            int insertedCount = 0;
            while (dirsToVisit.Count > 0)
            {
                var dirPath = dirsToVisit[0];
                dirsToVisit.RemoveAt(0);
                // accessing directory information might fail e.g. if we
                // don't have access permissions so we'll skip all 
                try
                {
                    DirectoryInfo dirInfo = new DirectoryInfo(dirPath);
                    foreach (var di in dirInfo.EnumerateDirectories())
                    {
                        dirsToVisit.Add(di.FullName);
                    }
                    foreach (var fi in dirInfo.EnumerateFiles())
                    {
                        var fe = new FileEntry
                        {
                            Path = fi.FullName,
                            Size = fi.Length,
                            CreationTimeUtc = fi.CreationTimeUtc,
                            LastAccessTimeUtc = fi.LastAccessTimeUtc,
                            LastWriteTimeUtc = fi.LastWriteTimeUtc
                        };
                        dbRoot.FileSizeIndex.Put(fe.Size, fe);
                        dbRoot.FileNameIndex.Put(fi.Name, fe);
                        dbRoot.FileLastWriteTimeIndex.Put(fe.LastWriteTimeUtc, fe);
                        ++insertedCount;
                        if (insertedCount % 10000 == 0)
                        {
                            Console.WriteLine(String.Format("Inserted {0} FileEntry objects", insertedCount));
                            db.Commit();
                        }
                    }
                }
                catch
                {
                }
            }
            // commit the changes if we're done creating a database
            db.Commit();
            // when we're finished, each index should have the same
            // number of items in it, equal to number of inserted objects
            //Debug.Assert(dbRoot.FileSizeIndex.Count == insertedCount);
            //Debug.Assert(dbRoot.FileNameIndex.Count == insertedCount);
            //Debug.Assert(dbRoot.FileLastWriteTimeIndex.Count == insertedCount);
        }

        static void ListSmallestFiles(DatabaseRoot dbRoot, int limit)
        {
            Console.WriteLine("\nThe smallest files:");
            // Indexes are ordered in ascending order i.e. smallest
            // values are first. Index implements GetEnumerator()
            // function which we can (implicitly) use in foreach loop:
            foreach (var fe in dbRoot.FileSizeIndex)
            {
                Console.WriteLine(String.Format("{0}: {1} bytes", fe.Path, fe.Size));
                if (--limit == 0)
                    break;
            }
        }

        static void ListBiggestFiles(DatabaseRoot dbRoot, int limit)
        {
            Console.WriteLine("\nThe biggest files:");
            // To list biggest files, we iterate the index in descending
            // order, using an enumerator returned by Reverse() function:
            foreach (var fe in dbRoot.FileSizeIndex.Reverse())
            {
                Console.WriteLine(String.Format("{0}: {1} bytes", fe.Path, fe.Size));
                if (--limit == 0)
                    break;
            }
        }

        static void ListMostRecentlyWrittenToFiles(DatabaseRoot dbRoot, int limit)
        {
            Console.WriteLine("\nThe most recently written-to files:");
            // the biggest DateTime values represent the most recent dates,
            // so once again we iterate the index in reverse (descent) order:
            foreach (var fe in dbRoot.FileLastWriteTimeIndex.Reverse())
            {
                Console.WriteLine(String.Format("{0}: {1} bytes", fe.Path, fe.Size));
                if (--limit == 0)
                    break;
            }
        }

        static void ListDuplicateNamesFiles(DatabaseRoot dbRoot, int limit)
        {
            Console.WriteLine("\nFiles with the same name:");
            string prevName = "";
            string prevPath = "";
            // The name of the file is not an explicit part of FileEntry
            // object, but since it's part of the index, we can access it
            // if we use IDictionaryEnumerator, which provides both the
            // key and 
            IDictionaryEnumerator de = dbRoot.FileNameIndex.GetDictionaryEnumerator();
            var dups = new Dictionary<string, bool>();
            while (de.MoveNext())
            {
                string name = (string)de.Key;
                FileEntry fe = (FileEntry)de.Value;
                if (name == prevName)
                {
                    bool firstDup = !dups.ContainsKey(name);
                    if (firstDup)
                    {
                        Console.WriteLine(prevPath);
                        Console.WriteLine(" " + fe.Path);
                        dups[name] = true;
                        if (--limit == 0)
                            break;
                    }
                    else
                    {
                        Console.WriteLine(" " + fe.Path);
                    }
                }
                prevName = name;
                prevPath = fe.Path;
            }
        }
    }
}
