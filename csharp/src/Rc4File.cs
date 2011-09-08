namespace Volante
{
    using System;
    using Volante.Impl;

    public class Rc4File : OsFile
    {
        public override void Write(long pos, byte[] buf)
        {
            if (pos > length)
            {
                if (zeroPage == null)
                {
                    zeroPage = new byte[Page.pageSize];
                    encrypt(zeroPage, 0, zeroPage, 0, Page.pageSize);
                }
                do
                {
                    base.Write(length, zeroPage);
                } while ((length += Page.pageSize) < pos);
            }

            if (pos == length)
                length += Page.pageSize;

            encrypt(buf, 0, cipherBuf, 0, buf.Length);
            base.Write(pos, cipherBuf);
        }

        public override int Read(long pos, byte[] buf)
        {
            if (pos < length)
            {
                int rc = base.Read(pos, buf);
                decrypt(buf, 0, buf, 0, rc);
                return rc;
            }
            return 0;
        }

        public Rc4File(String filePath, String key)
            : this(filePath, key, false)
        {
        }

        public Rc4File(String filePath, String key, bool readOnly)
            : base(filePath, readOnly)
        {
            length = file.Length & ~(Page.pageSize - 1);
            setKey(key);
        }

        private void setKey(String key)
        {
            for (int counter = 0; counter < 256; ++counter)
            {
                initState[counter] = (byte)counter;
            }
            int index1 = 0;
            int index2 = 0;
            int length = key.Length;
            for (int counter = 0; counter < 256; ++counter)
            {
                index2 = (key[index1] + initState[counter] + index2) & 0xff;
                byte temp = initState[counter];
                initState[counter] = initState[index2];
                initState[index2] = temp;
                index1 = (index1 + 1) % length;
            }
        }

        private void encrypt(byte[] clearText, int clearOff, byte[] cipherText, int cipherOff, int len)
        {
            x = y = 0;
            Array.Copy(initState, 0, state, 0, state.Length);
            for (int i = 0; i < len; i++)
            {
                cipherText[cipherOff + i] = (byte)(clearText[clearOff + i] ^ state[nextState()]);
            }
        }

        private void decrypt(byte[] cipherText, int cipherOff, byte[] clearText, int clearOff, int len)
        {
            x = y = 0;
            Array.Copy(initState, 0, state, 0, state.Length);
            for (int i = 0; i < len; i++)
            {
                clearText[clearOff + i] = (byte)(cipherText[cipherOff + i] ^ state[nextState()]);
            }
        }

        private int nextState()
        {
            x = (x + 1) & 0xff;
            y = (y + state[x]) & 0xff;
            byte temp = state[x];
            state[x] = state[y];
            state[y] = temp;
            return (state[x] + state[y]) & 0xff;
        }

        private byte[] cipherBuf = new byte[Page.pageSize];
        private byte[] initState = new byte[256];
        private byte[] state = new byte[256];
        private int x, y;
        private long length;
        private byte[] zeroPage;
    }
}
