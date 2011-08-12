namespace Volante
{
    /// <summary>
    /// This implementation of <code>IFile</code> interface can be used
    /// to make Perst an main-memory database. It should be used when pagePoolSize
    /// is set to <code>Storage.INFINITE_PAGE_POOL</code>. In this case all pages are cached in memory
    /// and <code>NullFile</code> is used just as a stub.
    /// <code>NullFile</code> should be used only when data is transient - i.e. it should not be saved
    /// between database sessions. If you need in-memory database but which provide data persistency, 
    /// you should use normal file and infinite page pool size. 
    /// </summary>
    public class NullFile : IFile
    {
        public void Write(long pos, byte[] buf) { }

        public int Read(long pos, byte[] buf)
        {
            return 0;
        }

        public void Sync() { }

        public void Lock() { }

        public void Close() { }

        public bool NoFlush
        {
            get { return false; }
            set { }
        }

        public long Length
        {
            get { return 0; }
        }
    }
}
