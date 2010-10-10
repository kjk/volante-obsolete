package org.nachodb.impl;
import  org.nachodb.*;
import java.io.*;

public class MultiFile implements IFile 
{ 
    static class MultiFileSegment { 
	RandomAccessFile f;
	String           name;
	long             size;
    }

    void seek(long pos) throws IOException {
	currSeg = 0;
	currOffs = 0;
	currPos = 0;
	while (pos > segment[currSeg].size) { 
	    currPos += segment[currSeg].size;
	    pos -= segment[currSeg].size;
	    currSeg += 1;
	}
	segment[currSeg].f.seek(pos);
	pos = segment[currSeg].f.getFilePointer();
	currOffs += pos;
	currPos += pos;
    }


    public void write(long pos, byte[] b) 
    {
        try { 
            seek(pos);
            int len = b.length;
            int off = 0;
            while (len > 0) { 
                int toWrite = len;
                if (len + currOffs > segment[currSeg].size) {
                    toWrite = (int)(segment[currSeg].size - currOffs);
                }
                segment[currSeg].f.write(b, off, toWrite);
                currPos += toWrite;
                currOffs += toWrite;
                off += toWrite;
                len -= toWrite;
                if (currOffs == segment[currSeg].size) {
                    segment[++currSeg].f.seek(0);
                    currOffs = 0;
                }
            }
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    public int read(long pos, byte[] b) 
    { 
        try { 
            seek(pos);
            int totalRead = 0;
            int len = b.length;
            int off = 0;
            while (len > 0) { 
                int toRead = len;
                if (len + currOffs > segment[currSeg].size) {
                    toRead = (int)(segment[currSeg].size - currOffs);
                }
                int rc = segment[currSeg].f.read(b, off, toRead);
                if (rc >= 0) { 
                    currPos += rc;
                    currOffs += rc;
                    totalRead += rc;
                    if (currOffs == segment[currSeg].size) {
                        segment[++currSeg].f.seek(0);
                        currOffs = 0;
                    }
                } else { 
                    return (totalRead == 0) ? rc : totalRead;
                }
                if (rc != toRead) { 
                    return totalRead;
                }
                off += rc;
                len -= rc;
            }
            return totalRead;
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }
        
    public void sync()
    { 
        if (!noFlush) { 
            try {   
                for (int i = segment.length; --i >= 0;) { 
                    segment[i].f.getFD().sync();
                }
            } catch(IOException x) { 
                throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
            }
        }
    }
    
    public boolean lock() 
    { 
        return OSFile.lockFile(segment[0].f);
    }

    public void close() 
    { 
        try { 
            for (int i = segment.length; --i >= 0;) { 
                segment[i].f.close();
            }
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    public MultiFile(String[] segmentPath, long[] segmentSize, boolean readOnly, boolean noFlush) { 
        this.noFlush = noFlush;
        segment = new MultiFileSegment[segmentPath.length];
        String mode = readOnly ? "r" : "rw";
        try { 
            for (int i = 0; i < segment.length; i++) { 
                MultiFileSegment seg = new MultiFileSegment();
                seg.f = new RandomAccessFile(segmentPath[i], mode);
                seg.size = segmentSize[i];
                fixedSize += seg.size;
                segment[i] = seg;
            }
            fixedSize -= segment[segment.length-1].size;
            segment[segment.length-1].size = Long.MAX_VALUE;
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    public MultiFile(String filePath, boolean readOnly, boolean noFlush) { 
        try { 
            StreamTokenizer in = 
                new StreamTokenizer(new BufferedReader(new FileReader(filePath)));
            this.noFlush = noFlush;
            String mode = readOnly ? "r" : "rw";
            segment = new MultiFileSegment[0];
            int tkn = in.nextToken();
            do {
                MultiFileSegment seg = new MultiFileSegment();
                if (tkn != StreamTokenizer.TT_WORD) { 
                    throw new IOException("Multifile segment name expected");
                }
                seg.name = in.sval;
                tkn = in.nextToken();
                if (tkn != StreamTokenizer.TT_EOF) { 
                    if (tkn != StreamTokenizer.TT_NUMBER) { 
                        throw new StorageError(StorageError.FILE_ACCESS_ERROR, "Multifile segment size expected");
                    }
                    seg.size = (long)in.nval*1024; // kilobytes
                    tkn = in.nextToken();
                }
                fixedSize += seg.size;
                seg.f = new RandomAccessFile(seg.name, mode);
                MultiFileSegment[] newSegment = new MultiFileSegment[segment.length+1];
                System.arraycopy(segment, 0, newSegment, 0, segment.length);
                newSegment[segment.length] = seg;
                segment = newSegment;
            } while (tkn != StreamTokenizer.TT_EOF);

            fixedSize -= segment[segment.length-1].size;
            segment[segment.length-1].size = Long.MAX_VALUE;
        } catch(IOException x) { 
            throw new StorageError(StorageError.FILE_ACCESS_ERROR, x);
        }
    }

    public long length() {
        try { 
            return fixedSize +  segment[segment.length-1].f.length();
        } catch (IOException x) {
            return -1;
        }
    }

    MultiFileSegment segment[];
    long             currPos;
    long             currOffs;
    long             fixedSize;
    int              currSeg;
    boolean          noFlush;
}
