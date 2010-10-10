package org.nachodb.impl;
import  org.nachodb.*;

public class Rc4File implements IFile 
{ 
    public void write(long pos, byte[] buf) 
    {
        if (pos > length) { 
            if (zeroPage == null) { 
                zeroPage = new byte[Page.pageSize];
                encrypt(zeroPage, 0, zeroPage, 0, Page.pageSize);
            }
            do { 
                file.write(length, zeroPage);
            } while ((length += Page.pageSize) < pos);
        } 
        if (pos == length) { 
            length += Page.pageSize;
        }        
        encrypt(buf, 0, cipherBuf, 0, buf.length);
        file.write(pos, cipherBuf);
    }

    public int read(long pos, byte[] buf) 
    { 
        if (pos < length) { 
            int rc = file.read(pos, buf);
            decrypt(buf, 0, buf, 0, rc);
            return rc;
        } 
        return 0;
    }

    public Rc4File(String filePath, boolean readOnly, boolean noFlush, String key) 
    { 
        file = new OSFile(filePath, readOnly, noFlush);
        length = file.length() & ~(Page.pageSize-1);
        setKey(key.getBytes());
    }

    public Rc4File(IFile file, String key) 
    { 
        this.file = file;
        length = file.length() & ~(Page.pageSize-1);
        setKey(key.getBytes());
    }

    private void setKey(byte[] key)
    {
	for (int counter = 0; counter < 256; ++counter) { 
	    initState[counter] = (byte)counter;
        }
	int index1 = 0;
	int index2 = 0;
	for (int counter = 0; counter < 256; ++counter) {
	    index2 = (key[index1] + initState[counter] + index2) & 0xff;
	    byte temp = initState[counter];
	    initState[counter] = initState[index2];
	    initState[index2] = temp;
	    index1 = (index1 + 1) % key.length;
        }
    }

    private final void encrypt(byte[] clearText, int clearOff, byte[] cipherText, int cipherOff, int len)
    {
        x = y = 0;
        System.arraycopy(initState, 0, state, 0, state.length);
	for (int i = 0; i < len; i++) {
	    cipherText[cipherOff + i] =
		(byte)(clearText[clearOff + i] ^ state[nextState()]);
        }
    }

    private final void decrypt(byte[] cipherText, int cipherOff, byte[] clearText, int clearOff, int len)
    {
        x = y = 0;
        System.arraycopy(initState, 0, state, 0, state.length);
	for (int i = 0; i < len; i++) {
	    clearText[clearOff + i] =
		(byte)(cipherText[cipherOff + i] ^ state[nextState()]);
	}
    }

    private final int nextState()
    {
	x = (x + 1) & 0xff;
	y = (y + state[x]) & 0xff;
	byte temp = state[x];
	state[x] = state[y];
	state[y] = temp;
	return (state[x] + state[y]) & 0xff;
    }

    public void close() { 
        file.close();
    }

    public boolean lock() { 
        return file.lock();
    }

    public void sync() { 
        file.sync();
    }

    public long length() {
        return file.length();
    }

    private IFile  file;
    private byte[] cipherBuf = new byte[Page.pageSize];
    private byte[] initState = new byte[256];
    private byte[] state = new byte[256];
    private int    x, y;
    private long   length;
    private byte[] zeroPage;
}
