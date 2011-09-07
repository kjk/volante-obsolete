namespace Volante
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using Volante.Impl;

    public class OsFile : IFile
    {
        public FileListener Listener { get; set; }

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
            if (Listener != null)
                Listener.OnWrite(pos, buf.Length);
        }

        public virtual int Read(long pos, byte[] buf)
        {
            file.Seek(pos, SeekOrigin.Begin);
            int len = file.Read(buf, 0, buf.Length);
            if (Listener != null)
                Listener.OnRead(pos, buf.Length, len);
            return len;
        }

#if NET_4_0
        [System.Security.SecuritySafeCritical]
#endif
        public virtual void Sync()
        {
            file.Flush();
#if !CF && !MONO && !SILVERLIGHT
            if (!NoFlush)
            {
                FlushFileBuffers(file.SafeFileHandle);
            }
#endif
            if (Listener != null)
                Listener.OnSync();
        }

        /// Whether to not flush file buffers during transaction commit. It will increase performance because
        /// it eliminates synchronous write to the disk. It can cause database corruption in case of 
        /// OS or power failure. Abnormal termination of application itself should not cause
        /// the problem, because all data written to a file but not yet saved to the disk is 
        /// stored in OS file buffers andwill be written to the disk.
        /// Default value: false
        public bool NoFlush { get; set; }

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


        public OsFile(String filePath)
            : this(filePath, false)
        {
        }

        public OsFile(String filePath, bool readOnly)
        {
            NoFlush = false;
            file = new FileStream(filePath, FileMode.OpenOrCreate,
                                  readOnly ? FileAccess.Read : FileAccess.ReadWrite);
        }

        protected FileStream file;
    }
}