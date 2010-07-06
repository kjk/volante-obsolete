namespace Perst.Impl    
{
    using System;
    using System.IO;
    using Perst;
	
    public class OSFile : IFile
    {
        public virtual void Write(long pos, byte[] buf)
        {
            file.Seek(pos, SeekOrigin.Begin);
            file.Write(buf, 0, buf.Length);
        }
		
        public virtual int Read(long pos, byte[] buf)
        {
            file.Seek(pos, SeekOrigin.Begin);
            return file.Read(buf, 0, buf.Length);
        }
		
        public virtual void  Sync()
        {
            file.Flush();
        }
		
        public virtual void  Close()
        {
            file.Close();
        }
		
        internal OSFile(String filePath)
        {
            file = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
        }
		
        private FileStream file;
    }
}