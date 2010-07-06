package org.garret.perst.impl;
import  org.garret.perst.*;

import java.io.*;

class FileIO { 
    final void write(long pos, byte[] buf) 
    {
	try { 
	    file.seek(pos);
	    file.write(buf, 0, buf.length);
	} catch(IOException x) { 
	    throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
	}
    }

     final int read(long pos, byte[] buf) 
     { 
	try { 
	    file.seek(pos);
	    return file.read(buf, 0, buf.length);
	} catch(IOException x) { 
	    throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
	}
    }
	
    final void sync()
    { 
	try { 	
            file.getFD().sync();
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
	}
    }
    
    final void close() 
    { 
	try { 
	    file.close();
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
	}
    }

    FileIO(String filePath) { 
	try { 
            file = new RandomAccessFile(filePath, "rw");
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
	}
    }

    private RandomAccessFile file;
}
