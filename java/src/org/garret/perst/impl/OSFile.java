package org.garret.perst.impl;
import  org.garret.perst.*;

import java.io.*;

public class OSFile implements IFile { 
    public void write(long pos, byte[] buf) 
    {
        try { 
            file.seek(pos);
            file.write(buf, 0, buf.length);
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    public int read(long pos, byte[] buf) 
    { 
        try { 
            file.seek(pos);
            return file.read(buf, 0, buf.length);
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }
        
    public void sync()
    { 
        try {   
            file.getFD().sync();
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }
    
    public void close() 
    { 
        try { 
            file.close();
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    public OSFile(String filePath) { 
        try { 
            file = new RandomAccessFile(filePath, "rw");
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    private RandomAccessFile file;
}
