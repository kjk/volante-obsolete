package org.nachodb;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import org.nachodb.impl.OSFile;

/**
 * Class using NIO mapping file on virtual mapping.
 * Useing this class instead standard OSFile can significantly increase
 * speed of application in some cases. 
 */
public class MappedFile implements IFile { 
    private final void checkSize(long size) throws IOException { 
        if (size > mapSize) { 
            long newSize = mapSize < Integer.MAX_VALUE/2 ? mapSize*2 : Integer.MAX_VALUE;
            if (newSize < size) { 
                newSize = size;
            }
            mapSize = newSize;
            map = chan.map(FileChannel.MapMode.READ_WRITE,
                           0, // position
                           mapSize);
        }
    }

    public void write(long pos, byte[] buf) 
    {
        try { 
            checkSize(pos + buf.length);
            map.position((int)pos);
            map.put(buf, 0, buf.length);
        } catch (IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    public int read(long pos, byte[] buf) 
    { 
        if (pos >= mapSize) { 
            return 0;
        }
        map.position((int)pos);
        map.get(buf, 0, buf.length);
        return buf.length;
    }
        
    public void sync()
    { 
        map.force();
    }
    
    public void close() 
    { 
        try { 
            chan.close();
            f.close();
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    public boolean lock() 
    {
        return OSFile.lockFile(f);
    }

    public long length() { 
        try { 
            return f.length();
        } catch(IOException x) { 
            return -1;
        }
    }

    public MappedFile(String filePath, long initialSize, boolean readOnly) { 
        try { 
            f = new RandomAccessFile(filePath, readOnly ? "r" : "rw");
            chan = f.getChannel();
            long size = chan.size();
            mapSize = (readOnly || size > initialSize) ? size : initialSize;
            map = chan.map(readOnly
                           ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE,
                           0, // position
                           mapSize);
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    RandomAccessFile f;
    MappedByteBuffer map;
    FileChannel      chan;
    long             mapSize;
}
