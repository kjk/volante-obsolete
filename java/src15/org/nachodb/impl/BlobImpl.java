package org.nachodb.impl;
import  org.nachodb.*;

public class BlobImpl extends PersistentResource implements Blob { 
    int      size;
    BlobImpl next;
    byte[]   body;

    static class BlobInputStream extends java.io.InputStream {
        protected BlobImpl curr;
        protected int      pos;
        protected int      rest;

        public int read() {
            byte[] b = new byte[1];
            return read(b, 0, 1) == 1 ? b[0] & 0xFF : -1;
        }

        public int read(byte b[], int off, int len) {
            if (len > rest) { 
                len = rest;
            }
            int beg = off;
            while (len > 0) { 
                if (pos == curr.body.length) { 
                    BlobImpl prev = curr;
                    curr = curr.next;
                    curr.load();
                    prev.invalidate();
                    prev.next = null;
                    pos = 0;
                }
                int n = len > curr.body.length - pos ? curr.body.length - pos : len; 
                System.arraycopy(curr.body, pos, b, off, n);
                pos += n;
                off += n;
                len -= n;
                rest -= n;
            }
            return off - beg;
        }

        public long skip(long offs) {
            if (offs > rest) { 
                offs = rest;
            }
            int len = (int)offs;
            while (len > 0) { 
                if (pos == curr.body.length) { 
                    BlobImpl prev = curr;
                    curr = curr.next;
                    curr.load();
                    prev.invalidate();
                    prev.next = null;
                    pos = 0;
                }
                int n = len > curr.body.length - pos ? curr.body.length - pos : len; 
                pos += n;
                len -= n;
                rest -= n;
            }
            return offs;
        }


        public int available() {
            return rest;
        }

        public void close() {
            curr = null;
            rest = 0;
        }

        protected BlobInputStream(BlobImpl first) { 
            first.load();
            curr = first;
            rest = first.size;
        }
    }

    static class BlobOutputStream extends java.io.OutputStream { 
        protected BlobImpl first;
        protected BlobImpl curr;
        protected int      pos;
        protected boolean  multisession;

        public void write(int b) { 
            byte[] buf = new byte[1];
            buf[0] = (byte)b;
            write(buf, 0, 1);
        }

        public void write(byte b[], int off, int len) { 
            while (len > 0) { 
                if (pos == curr.body.length) { 
                    BlobImpl next = new BlobImpl(curr.getStorage(), curr.body.length);
                    BlobImpl prev = curr;
                    curr = prev.next = next;
                    if (prev != first) {
                        prev.store();
                        prev.invalidate();
                        prev.next = null; 
                    }
                    pos = 0;
                }
                int n = len > curr.body.length - pos ? curr.body.length - pos : len;  
                System.arraycopy(b, off, curr.body, pos, n);
                off += n;
                pos += n;
                len -= n;
                first.size += n;
            }
        }

        public void close() {
            if (!multisession && pos < curr.body.length) { 
                byte[] tmp = new byte[pos];
                System.arraycopy(curr.body, 0, tmp, 0, pos);
                curr.body = tmp;
            }
            curr.store();
            if (curr != first) {
                first.store();
            }
            first = curr = null;
        }

        BlobOutputStream(BlobImpl first, boolean multisession) { 
            first.load();
            this.first = first;
            this.multisession = multisession;
            int size = first.size;
            while (first.next != null) { 
                size -= first.body.length;
                BlobImpl prev = first;
                first = first.next;                
                first.load();
                prev.invalidate();
                prev.next = null;
                pos = 0;
                
            }
            curr = first;
            pos = size;
        }
    }

    public boolean recursiveLoading() { 
        return false;
    }

    /**
     * Get input stream. InputStream.available method can be used to get BLOB size
     * @return input stream with BLOB data
     */
    public java.io.InputStream getInputStream() { 
        return new BlobInputStream(this);
    }

    /**
     * Get output stream to append data to the BLOB.
     * @return output stream
     */
    public java.io.OutputStream getOutputStream() { 
        return new BlobOutputStream(this, true);
    }

   /**
     * Get output stream to append data to the BLOB.
     * @param multisession whether BLOB allows further appends of data or closing 
     * this output stream means that BLOB will not be changed any more. 
     * @return output stream 
     */
    public java.io.OutputStream getOutputStream(boolean multisession) { 
        return new BlobOutputStream(this, multisession);
    }

    public void deallocate() { 
        load();
        if (size > 0) {
            BlobImpl curr = next;
            while (curr != null) { 
                curr.load();
                BlobImpl tail = curr.next;
                curr.deallocate();
                curr = tail;
            }
        }
        super.deallocate();
    }

    BlobImpl(Storage storage, int size) { 
        super(storage);
        body = new byte[size];
    }

    BlobImpl() {}
}   