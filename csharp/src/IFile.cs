namespace Volante
{
    /// <summary>Allows getting notifications for IFile.Write(), IFile.Read() and
    /// IFile.Sync() calls. Useful as a debugging/diagnostic tool.
    /// </summary>
    public interface IFileMonitor
    {
        void OnWrite(long pos, long len);
        void OnRead(long pos, long bufSize, long read);
        void OnSync();
    }

    /// <summary>Interface for a database file.
    /// Programmer can provide its own implementation of this interface, adding such features
    /// as support encryption, compression etc.
    /// Implentations should throw DatabaseException exception in case of failure.
    /// </summary>
    public interface IFile
    {
        /// <summary>Write data to the file
        /// </summary>
        /// <param name="pos">offset in the file
        /// </param>
        /// <param name="buf">array with data to be writter (size is always equal to database page size)
        /// </param>
        /// 
        void Write(long pos, byte[] buf);

        /// <summary>Read data from the file
        /// </summary>
        /// <param name="pos">offset in the file
        /// </param>
        /// <param name="buf">array to receive data (size is always equal to database page size)
        /// </param>
        /// <returns>number of bytes read
        /// </returns>
        int Read(long pos, byte[] buf);

        /// <summary>Flush all file changes to disk
        /// </summary>
        void Sync();

        /// <summary>
        /// Prevent other processes from modifying the file
        /// </summary>
        void Lock();

        /// <summary>Close the file
        /// </summary>
        void Close();

        /// <summary>
        /// Set to <code>true</code> to avoid flushing the stream, or <c>false</c> to flush the stream with every call to <see cref="Sync"/>
        /// </summary>
        bool NoFlush { get; set ;}

        /// <summary>
        /// Length of the file
        /// </summary>
        /// <returns>length of file in bytes</returns>
        long Length { get; }

        /// <summary>
        /// Get/set <code>IFileMonitor</code> object
        /// </summary>
        IFileMonitor Monitor { get; set; }
    }
}
