package org.nachodb.impl;
import  org.nachodb.*;

import java.lang.reflect.*;
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
        if (!noFlush) { 
            try {   
                file.getFD().sync();
            } catch(IOException x) { 
                throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
            }
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

    public boolean lock() 
    { 
        return lockFile(file);
    }

    static final long MAX_FILE_SIZE = Long.MAX_VALUE-2;

    public static boolean lockFile(RandomAccessFile file) 
    { 
	try { 
	    Class cls = file.getClass();
	    Method getChannel = cls.getMethod("getChannel", new Class[0]);
	    if (getChannel != null) { 
		Object channel = getChannel.invoke(file, new Object[0]);
		if (channel != null) { 
		    cls = channel.getClass();
		    Class[] paramType = new Class[3];
		    paramType[0] = Long.TYPE;
		    paramType[1] = Long.TYPE;
		    paramType[2] = Boolean.TYPE;
		    Method lock = cls.getMethod("tryLock", paramType);
		    if (lock != null) { 
			Object[] param = new Object[3];
			param[0] = new Long(MAX_FILE_SIZE);
			param[1] = new Long(1);
			param[2] = new Boolean(false);
			return lock.invoke(channel, param) != null;
		    }
		}
	    }
	} catch (Exception x) {}

	return true;
    }

    public OSFile(String filePath, boolean readOnly, boolean noFlush) { 
        this.noFlush = noFlush;
        try { 
            file = new RandomAccessFile(filePath, readOnly ? "r" : "rw");
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    public long length() {
        try { 
            return file.length();
        } catch (IOException x) { 
            return -1;
        }
    }


    protected RandomAccessFile file;
    protected boolean          noFlush;
}
