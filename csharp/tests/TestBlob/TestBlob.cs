using System;
using NachoDB;
using System.IO;

public class TestBlob 
{ 
    public static void Main(string[] args) 
    { 
        Storage db = StorageFactory.Instance.CreateStorage();
        db.Open("testblob.dbs");
        byte[] buf = new byte[1024];
        int rc;
        string[] files = Directory.GetFiles(@"src\impl", "*.cs");
#if USE_GENERICS
        Index<string,Blob> root = (Index<string,Blob>)db.Root;
#else
        Index root = (Index)db.Root;
#endif
        if (root == null) 
        { 
#if USE_GENERICS
            root = db.CreateIndex<string,Blob>(true);
#else
            root = db.CreateIndex(typeof(string), true);
#endif
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
#if USE_GENERICS
            Blob blob = root[file];
#else
            Blob blob = (Blob)root[file];
#endif
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

