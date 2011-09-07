namespace Volante
{
    /// <summary>
    /// IFile implementation which to store databases on <see cref="System.IO.Stream"/> instances.
    /// </summary>
    public class StreamFile : IFile
    {
        private long offset = 0;
        private System.IO.Stream stream;

        public FileListener Listener { get; set; }

        /// <summary>
        /// Construction
        /// </summary>
        /// <param name="stream">A <see cref="System.IO.Stream"/> where to store the database</param>
        public StreamFile(System.IO.Stream stream)
        {
            this.stream = stream;
        }

        /// <summary>
        /// Construction
        /// </summary>
        /// <param name="stream">A <see cref="System.IO.Stream"/> where to store the database</param>
        /// <param name="offset">Offset within the stream where to store/find the database</param>
        public StreamFile(System.IO.Stream stream, long offset)
        {
            this.stream = stream;
            this.offset = offset;
        }

        /// <summary>
        /// Write method
        /// </summary>
        /// <param name="pos">Zero-based position</param>
        /// <param name="buf">Buffer to write to the stream. The entire buffer is written</param>
        public void Write(long pos, byte[] buf)
        {
            stream.Position = pos + offset;
            stream.Write(buf, 0, buf.Length);
            if (Listener != null)
                Listener.OnWrite(pos, buf.Length);
        }

        /// <summary>
        /// Read method
        /// </summary>
        /// <param name="pos">Zero-based position</param>
        /// <param name="buf">Buffer where to store <c>buf.Length</c> byte(s) read from the stream</param>
        public int Read(long pos, byte[] buf)
        {
            stream.Position = pos + offset;
            int len = stream.Read(buf, 0, buf.Length);
            if (Listener != null)
                Listener.OnRead(pos, buf.Length, len);
            return len;
        }

        /// <summary>
        /// Flushes the stream (subject to the NoFlush property)
        /// </summary>

        public void Sync()
        {
            if (NoFlush == false)
                stream.Flush();
            if (Listener != null)
                Listener.OnSync();
        }

        /// <summary>
        /// Closes the stream (subject to the NoFlush property)
        /// </summary>
        public void Close()
        {
            stream.Close();
        }

        /// <summary>
        /// Locks the stream (no-op)
        /// </summary>
        public void Lock()
        {
        }

        /// <summary>
        /// Boolean property. Set to <c>true</c> to avoid flushing the stream, or <c>false</c> to flush the stream with every calls to <see cref="Sync"/>
        /// </summary>
        public bool NoFlush { get; set; }

        public long Length
        {
            get { return stream.Length; }
        }
    }
}
