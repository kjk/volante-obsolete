namespace Volante
{
    using System;
    using System.Collections;
    using System.IO;

    public class TestBlobResult : TestResult
    {
        public TimeSpan InsertTime;
        public TimeSpan ReadTime;
    }

    public class TestBlob : ITest
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
            db.Commit();
            db.Close();
        }

        public void TestBlobImpl(TestConfig config)
        {
            int n;
            IDatabase db = config.GetDatabase(false);
            IBlob blob = db.CreateBlob();
            Stream blobStrm = blob.GetStream();

            byte[] b = new byte[] { 1, 2, 3, 4, 5, 6};
            byte[] b2 = new byte[6];
            blobStrm.Write(b, 0, b.Length);
            Tests.Assert(blobStrm.CanRead);
            Tests.Assert(blobStrm.CanSeek);
            Tests.Assert(blobStrm.CanWrite);
            long len = 6;
            long pos = 3;
            Tests.Assert(blobStrm.Length == len);
            blobStrm.Flush();
            Tests.Assert(6 == blobStrm.Position);
            blobStrm.Position = pos;
            Tests.Assert(pos == blobStrm.Position);
            Tests.AssertException<ArgumentException>(() =>
                { blobStrm.Position = -1; });
            blobStrm.Seek(0, SeekOrigin.Begin);
            Tests.Assert(0 == blobStrm.Position);
            n = blobStrm.Read(b2, 0, 6);
            Tests.Assert(n == 6);
            Tests.Assert(Tests.ByteArraysEqual(b, b2));
            Tests.Assert(6 == blobStrm.Position);
            n = blobStrm.Read(b2, 0, 1);
            Tests.Assert(n == 0);
            blobStrm.Seek(0, SeekOrigin.Begin);
            blobStrm.Seek(3, SeekOrigin.Current);
            Tests.Assert(3 == blobStrm.Position);
            blobStrm.Read(b2, 0, 3);
            Tests.Assert(6 == blobStrm.Position);
            Tests.Assert(b2[0] == 4);
            blobStrm.Seek(-3, SeekOrigin.End);
            Tests.Assert(3 == blobStrm.Position);
            Tests.AssertException<ArgumentException>(() =>
                { blobStrm.Seek(-10, SeekOrigin.Current); });
            blobStrm.Seek(0, SeekOrigin.End);
            Tests.Assert(len == blobStrm.Position);
            blobStrm.Write(b, 0, b.Length);
            len += b.Length;
            Tests.Assert(blobStrm.Length == len);
            blobStrm.SetLength(8);
            Tests.Assert(blobStrm.Length == 8);
            blobStrm.SetLength(20);
            Tests.Assert(blobStrm.Length == 20);
            blob.Deallocate();
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

            TestBlobImpl(config);
        }
    }

}
