namespace Volante.Impl
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using Volante;

    public class OSFile : IFile
    {
#if !MONO && !CF && !SILVERLIGHT
#if NET_4_0
        [System.Security.SecuritySafeCritical]
#endif
        [DllImport("kernel32.dll", SetLastError = true)]
        static extern int FlushFileBuffers(Microsoft.Win32.SafeHandles.SafeFileHandle fileHandle);
#endif
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

#if NET_4_0
        [System.Security.SecuritySafeCritical]
#endif
        public virtual void Sync()
        {
            file.Flush();
#if !CF && !MONO && !SILVERLIGHT
            if (!noFlush)
            {
                FlushFileBuffers(file.SafeFileHandle);
            }
#endif
        }

        public bool NoFlush
        {
            get { return this.noFlush; }
            set { this.noFlush = value; }
        }

        public virtual void Close()
        {
            file.Close();
        }

        public virtual void Lock()
        {
#if !CF
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
        protected bool noFlush;
    }
}