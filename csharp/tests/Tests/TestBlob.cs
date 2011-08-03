namespace Volante
{
    using System;
    using System.IO;

    public class TestBlobResult : TestResult
    {
        public TimeSpan InsertTime;
        public TimeSpan ReadTime;
    }

    public class TestBlob
    {
        static string DbName = "testblob.dbs";

        static string IsSrcImpl(string d)
        {
            d = Path.Combine(d, "src");
            if (Directory.Exists(d))
                return Path.Combine(d, "impl");
            return null;
        }

        static string FindSrcImplDirectory()
        {
            string curr = "";
            for (int i=0; i<6; i++)
            {
                string d = IsSrcImpl(curr);
                if (null != d)
                    return d;
                curr = Path.Combine("..", curr);
            }
            return null;
        }

        static void InsertFiles(string[] files)
        {
            int rc;
            byte[] buf = new byte[1024];
            Storage db = StorageFactory.CreateStorage();
            db.Open(DbName);
            Index<string, Blob> root = (Index<string, Blob>)db.Root;
            Tests.Assert(root == null);
            root = db.CreateIndex<string, Blob>(true);
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
            db.Close();
        }

        static void VerifyFiles(string[] files)
        {
            int rc;
            byte[] buf = new byte[1024];
            Storage db = StorageFactory.CreateStorage();
            db.Open(DbName);
            Index<string, Blob> root = (Index<string, Blob>)db.Root;
            Tests.Assert(root != null);
            foreach (string file in files)
            {
                byte[] buf2 = new byte[1024];
                Blob blob = root[file];
                Tests.AssertThat(blob != null);
                Stream bin = blob.GetStream();
                FileStream fin = new FileStream(file, FileMode.Open, FileAccess.Read);
                while ((rc = fin.Read(buf, 0, buf.Length)) > 0)
                {
                    int rc2 = bin.Read(buf2, 0, buf2.Length);
                    Tests.AssertThat(rc == rc2);
                    if (rc != rc2)
                        break;
                    while (--rc >= 0 && buf[rc] == buf2[rc]) ;
                    Tests.AssertThat(rc < 0);
                    if (rc >= 0)
                        break;
                }
                fin.Close();
                bin.Close();
            }
            db.Close();
        }

        public static TestBlobResult Run()
        {
            Tests.SafeDeleteFile(DbName);
            var res = new TestBlobResult()
            {
                TestName = "TestBlob"
            };
            string dir = FindSrcImplDirectory();
            if (null == dir)
            {
                res.Ok = false;
                return res;
            }
            string[] files = Directory.GetFiles(dir, "*.cs");
            res.Count = files.Length;

            var tStart = DateTime.Now;
            var start = DateTime.Now;
            InsertFiles(files);
            res.InsertTime = DateTime.Now - start;

            start = DateTime.Now;
            VerifyFiles(files);
            res.ReadTime = DateTime.Now - start;

            res.ExecutionTime = DateTime.Now - tStart;
            res.Ok = Tests.FinalizeTest();
            return res;
        }
    }

}
