namespace Perst
{
    /// <summary> Interface of file.
    /// Prorgemmer can provide its own impleentation of this interface, adding such features
    /// as support of flash cards, encrypted files,...
    /// Implentation of this interface should throw StorageError exception in case of failure
    /// </summary>
    public interface IFile 
    { 
        /// <summary> Write data to the file
        /// </summary>
        /// <param name="pos"> offset in the file
        /// </param>
        /// <param name="buf"> array with data to be writter (size is always equal to database page size)
        /// </param>
        /// 
        void Write(long pos, byte[] buf);

        /// <summary> Reade data from the file
        /// </summary>
        /// <param name="pos"> offset in the file
        /// </param>
        /// <param name="buf"> array to receive readen data (size is always equal to database page size)
        /// </param>
        /// <returns> param number of bytes actually readen
        /// </returns>
        int Read(long pos, byte[] buf);

        /// <summary> Flush all fiels changes to the disk
        /// </summary>
        void Sync();
    
        /// <summary>
        /// Prevent other processes from modifying the file
        /// </summary>
        void Lock();

        /// <summary> Close file
        /// </summary>
        void Close();

        /// <summary>
        /// Boolean property. Set to <c>true</c> to avoid flushing the stream, or <c>false</c> to flush the stream with every calls to <see cref="Sync"/>
        /// </summary>
        bool NoFlush
        {
            get;
            set;
        }
    }
}
