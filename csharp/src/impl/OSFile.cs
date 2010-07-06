namespace Perst.Impl    
{
    using System;
    using System.IO;
    using Perst;
	
    public class OSFile : IFile
    {
        public void write(long pos, byte[] buf)
        {
            try
            {
                file.Seek(pos, SeekOrigin.Begin);
                file.Write(buf, 0, buf.Length);
            }
            catch (System.IO.IOException x)
            {
                throw new StorageError(StorageError.ErrorCode.FILE_ACCESS_ERROR, x);
            }
        }
		
        public int read(long pos, byte[] buf)
        {
            try
            {
                file.Seek(pos, SeekOrigin.Begin);
                return file.Read(buf, 0, buf.Length);
            }
            catch (IOException x)
            {
                throw new StorageError(StorageError.ErrorCode.FILE_ACCESS_ERROR, x);
            }
        }
		
        public void  sync()
        {
            try
            {
                file.Flush();
            }
            catch (IOException x)
            {
                throw new StorageError(StorageError.ErrorCode.FILE_ACCESS_ERROR, x);
            }
        }
		
        public void  close()
        {
            try
            {
                file.Close();
            }
            catch (IOException x)
            {
                throw new StorageError(StorageError.ErrorCode.FILE_ACCESS_ERROR, x);
            }
        }
		
        internal OSFile(String filePath)
        {
            try
            {
                file = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            }
            catch (IOException x)
            {
                throw new StorageError(StorageError.ErrorCode.FILE_ACCESS_ERROR, x);
            }
        }
		
        private FileStream file;
    }
}