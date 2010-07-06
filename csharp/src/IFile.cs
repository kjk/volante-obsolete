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
        void write(long pos, byte[] buf);

        /// <summary> Reade data from the file
        /// </summary>
        /// <param name="pos"> offset in the file
        /// </param>
        /// <param name="buf"> array to receive readen data (size is always equal to database page size)
        /// </param>
        /// <returns> param number of bytes actually readen
        /// </returns>
        int read(long pos, byte[] buf);

        /// <summary> Flush all fiels changes to the disk
        /// </summary>
        void sync();
    
        /// <summary> Close file
        /// </summary>
        void close();
    }
}
