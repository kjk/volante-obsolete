namespace Perst.Impl    
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using Perst;
	

    public class OSFile : IFile
    {
        [DllImport("kernel32.dll", SetLastError=true)] 
        static extern int FlushFileBuffers(int hFile); 

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
#if !COMPACT_NET_FRAMEWORK 
            if (!noFlush) { 
                FlushFileBuffers(file.Handle.ToInt32());
            }
#endif
        }

        public bool NoFlush
        {
            get { return this.noFlush; }
            set { this.noFlush = value; }
        }
        
        public virtual void  Close()
        {
            file.Close();
        }
		
        public virtual void Lock() 
        {
#if !COMPACT_NET_FRAMEWORK 
            file.Lock(0, long.MaxValue);
#endif
        }

        internal OSFile(String filePath, bool readOnly, bool noFlush)
        {
            this.noFlush = noFlush;
            file = new FileStream(filePath, FileMode.OpenOrCreate, 
                                  readOnly ? FileAccess.Read : FileAccess.ReadWrite);
        }
		
        protected FileStream file;
        protected bool       noFlush;
    }
}