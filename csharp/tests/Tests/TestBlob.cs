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

        void InsertFiles(TestConfig config, string[] files)
        {
            int rc;
            byte[] buf = new byte[1024];
            IDatabase db = config.GetDatabase();
            IIndex<string, IBlob> root = (IIndex<string, IBlob>)db.Root;
            Tests.Assert(root == null);
            root = db.CreateIndex<string, IBlob>(IndexType.Unique);
            db.Root = root;
            foreach (string file in files)
            {
                FileStream fin = new FileStream(file, FileMode.Open, FileAccess.Read);
                IBlob blob = db.CreateBlob();
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

        void VerifyFiles(TestConfig config, string[] files)
        {
            int rc;
            byte[] buf = new byte[1024];
            IDatabase db = config.GetDatabase(false);
            IIndex<string, IBlob> root = (IIndex<string, IBlob>)db.Root;
            Tests.Assert(root != null);
            foreach (string file in files)
            {
                byte[] buf2 = new byte[1024];
                IBlob blob = root[file];
                Tests.Assert(blob != null);
                Stream bin = blob.GetStream();
                FileStream fin = new FileStream(file, FileMode.Open, FileAccess.Read);
                while ((rc = fin.Read(buf, 0, buf.Length)) > 0)
                {
                    int rc2 = bin.Read(buf2, 0, buf2.Length);
                    Tests.Assert(rc == rc2);
                    if (rc != rc2)
                        break;
                    while (--rc >= 0 && buf[rc] == buf2[rc]) ;
                    Tests.Assert(rc < 0);
                    if (rc >= 0)
                        break;
                }
                fin.Close();
                bin.Close();
            }
            db.Close();
        }

        public void Run(TestConfig config)
        {
            var res = new TestBlobResult();
            config.Result = res;
            string dir = FindSrcImplDirectory();
            if (null == dir)
            {
                res.Ok = false;
                return;
            }
            string[] files = Directory.GetFiles(dir, "*.cs");
            res.Count = files.Length;

            var start = DateTime.Now;
            InsertFiles(config, files);
            res.InsertTime = DateTime.Now - start;

            start = DateTime.Now;
            VerifyFiles(config, files);
            res.ReadTime = DateTime.Now - start;
        }
    }

}
