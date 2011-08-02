namespace Volante.Impl    
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using Volante;

    public class OSFile : IFile
    {
        [DllImport("kernel32.dll", SetLastError=true)]
        // TODO: this seems more correct but doesn't work in mono
        //static extern bool FlushFileBuffers(IntPtr hFile); 
        static extern bool FlushFileBuffers(int hFile); 

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
#if !MONO
            if (!noFlush) {
                // TODO: this seems more correct but doesn't work in mono
                //FlushFileBuffers(file.SafeFileHandle.DangerousGetHandle());
                // TODO: this uses deprecated API but I can't get SafeFileHandle
                // to work in mono
                FlushFileBuffers(file.Handle.ToInt32());
            }
#endif
#endif
        }
™
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

        public long Length
        {
            get { return file.Length; }
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