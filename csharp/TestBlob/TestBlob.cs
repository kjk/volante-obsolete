using System;
using Perst;
using System.IO;

public class TestBlob 
{ 
    public static void Main(string[] args) 
    { 
        Storage db = StorageFactory.Instance.CreateStorage();
        db.Open("testblob.dbs");
        byte[] buf = new byte[1024];
        int rc;
        string[] files = Directory.GetFiles("\\Perst.NET\\src\\impl", "*.cs");
        Index root = (Index)db.Root;
        if (root == null) 
        { 
            root = db.CreateIndex(typeof(string), true);
            db.Root = root;
            foreach (String file in files) 
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
        foreach (String file in files) 
        {
            byte[] buf2 = new byte[1024];
            Blob blob = (Blob)root[file];
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

