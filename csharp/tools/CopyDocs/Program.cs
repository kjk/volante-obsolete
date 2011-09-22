using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace CopyDocs
{
    class Program
    {
        // we expect our source code is checked out under
        // "volante" directory
        static string FindSrcRooDir()
        {
            var path = Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
            var parts = path.Split(new char[1] { Path.DirectorySeparatorChar });
            int n = parts.Length;
            while (n > 0)
            {
                if (parts[--n] == "volante")
                {
                    // hack to work around bug (?) in Path.Combine() which
                    // combines "C:", "foo" as "c:foo" and not "c:\foo"
                    var d = parts[0] + Path.DirectorySeparatorChar;
                    var r = new string[n-1];
                    Array.Copy(parts, 1, r, 0, r.Length);
                    foreach (var rp in r)
                    {
                        d = Path.Combine(d, rp);
                    }
                    EnsureDirExists(d);
                    return d;
                }
            }
            throw new Exception("Couldn't find directory");
        }

        static void EnsureDirExists(string dir)
        {
            if (!Directory.Exists(dir))
                throw new Exception(String.Format("Dir {0} doesn't exist", dir));
        }

        static string FindSrcDocsDir()
        {
            var path = FindSrcRooDir();
            path = Path.Combine(path, "volante", "csharp", "doc");
            EnsureDirExists(path);
            return path;
        }

        static string FindDstDocsDir()
        {
            var path = FindSrcRooDir();
            path = Path.Combine(path, "web", "blog", "www", "software");
            EnsureDirExists(path);
            path = Path.Combine(path, "volante");
            var pathTmp = Path.Combine(path, "js");
            if (!Directory.Exists(pathTmp))
                Directory.CreateDirectory(pathTmp);
            EnsureDirExists(path);
            return path;
        }

        static void DeleteFilesInDir(string dir)
        {
            foreach (var path in Directory.GetFiles(dir))
            {
                File.Delete(path);
            }
        }

        static bool ShouldCopy(string fileName)
        {
            var ext = Path.GetExtension(fileName).ToLower();
            return ext == ".html" || ext == ".css" || ext == ".js";
        }

        static Dictionary<string, string> FileNameSubst = new Dictionary<string, string>() { 
            { "index.html", "database.html" }
        };

        static Dictionary<string, string> StrSubst = new Dictionary<string, string>() {
            { "<span id=gplus></span>", @"<span style='position:relative; left: 22px; top: 6px;'>
		<script type='text/javascript' src='http://apis.google.com/js/plusone.js'></script>
		<g:plusone size='medium' href='http://blog.kowalczyk.info/software/volante/'>
		</g:plusone>
		</span>" },
            { "<span id=adsense></span>", @"<script type='text/javascript'> 
  var _gaq = _gaq || [];
  _gaq.push(['_setAccount', 'UA-194516-1']);
  _gaq.push(['_trackPageview']);
 
  (function() {
    var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;
    ga.src = ('https:' == document.location.protocol ? 'https://ssl' : 'http://www') + '.google-analytics.com/ga.js';
    (document.getElementsByTagName('head')[0] || document.getElementsByTagName('body')[0]).appendChild(ga);
  })();
</script> " },
            { "href=\"index.html\"", "href=\"database.html\"" },
            { "href=index.html", "href=\"database.html\"" }
        };

        static bool NoSubst(string path)
        {
            var ext = Path.GetExtension(path).ToLower();
            return ext == ".css" || ext == ".js";
        }

        static void CopyFile(string srcPath, string dstPath)
        {
            if (NoSubst(srcPath))
            {
                File.Copy(srcPath, dstPath);
                return;
            }

            string contentOrig = File.ReadAllText(srcPath);
            string content = contentOrig;
            foreach (var strOld in StrSubst.Keys)
            {
                var strNew = StrSubst[strOld];
                content = content.Replace(strOld, strNew);
            }

            if (content == contentOrig)
                File.Copy(srcPath, dstPath);
            else
                File.WriteAllText(dstPath, content, Encoding.UTF8);
        }

        static void CopyFilesInDir(string srcDir, string dstDir)
        {
            var srcFiles = Directory.GetFiles(srcDir);
            foreach (var filePath in srcFiles)
            {
                var fileName = Path.GetFileName(filePath);
                if (!ShouldCopy(fileName))
                    continue;
                var srcPath = Path.Combine(srcDir, fileName);
                string dstFileName = null;
                if (!FileNameSubst.TryGetValue(fileName, out dstFileName))
                    dstFileName = fileName;
                var dstPath = Path.Combine(dstDir, dstFileName);
                CopyFile(srcPath, dstPath);
                Console.WriteLine(String.Format("{0} =>\n{1}\n", srcPath, dstPath));
            }
        }

        static void Main(string[] args)
        {
            string srcDir = FindSrcDocsDir();
            string dstDir = FindDstDocsDir();
            DeleteFilesInDir(dstDir); // we want a clean slate - no leaving of obsolete files
            CopyFilesInDir(srcDir, dstDir);
            srcDir = Path.Combine(srcDir, "js");
            dstDir = Path.Combine(dstDir, "js");
            DeleteFilesInDir(dstDir);
            CopyFilesInDir(srcDir, dstDir);
        }
    }
}
