namespace Volante.Impl
{
    using System;
    using System.Text;

    public class ByteBuffer
    {
        public void extend(int size)
        {
            if (size > arr.Length)
            {
                int newLen = size > arr.Length * 2 ? size : arr.Length * 2;
                byte[] newArr = new byte[newLen];
                Array.Copy(arr, 0, newArr, 0, used);
                arr = newArr;
            }
            used = size;
        }

        public byte[] toArray()
        {
            byte[] result = new byte[used];
            Array.Copy(arr, 0, result, 0, used);
            return result;
        }

        public int packI1(int offs, int val)
        {
            extend(offs + 1);
            arr[offs++] = (byte)val;
            return offs;
        }

        public int packBool(int offs, bool val)
        {
            extend(offs + 1);
            arr[offs++] = (byte)(val ? 1 : 0);
            return offs;
        }

        public int packI2(int offs, int val)
        {
            extend(offs + 2);
            Bytes.pack2(arr, offs, (short)val);
            return offs + 2;
        }

        public int packI4(int offs, int val)
        {
            extend(offs + 4);
            Bytes.pack4(arr, offs, val);
            return offs + 4;
        }

        public int packI8(int offs, long val)
        {
            extend(offs + 8);
            Bytes.pack8(arr, offs, val);
            return offs + 8;
        }

        public int packF4(int offs, float val)
        {
            extend(offs + 4);
            Bytes.packF4(arr, offs, val);
            return offs + 4;
        }

        public int packF8(int offs, double val)
        {
            extend(offs + 8);
            Bytes.packF8(arr, offs, val);
            return offs + 8;
        }

        public int packDecimal(int offs, decimal val)
        {
            extend(offs + 16);
            Bytes.packDecimal(arr, offs, val);
            return offs + 16;
        }

        public int packGuid(int offs, Guid val)
        {
            extend(offs + 16);
            Bytes.packGuid(arr, offs, val);
            return offs + 16;
        }

        public int packDate(int offs, DateTime val)
        {
            extend(offs + 8);
            Bytes.packDate(arr, offs, val);
            return offs + 8;
        }

        public int packString(int offs, string s)
        {
            if (s == null)
            {
                extend(offs + 4);
                Bytes.pack4(arr, offs, -1);
                offs += 4;
                return offs;
            }

            byte[] bytes = Encoding.UTF8.GetBytes(s);
            extend(offs + 4 + bytes.Length);
            Bytes.pack4(arr, offs, -2 - bytes.Length);
            Array.Copy(bytes, 0, arr, offs + 4, bytes.Length);
            offs += 4 + bytes.Length;
            return offs;
        }

        public ByteBuffer()
        {
            arr = new byte[64];
        }

        internal byte[] arr;
        internal int used;
    }
}