namespace Perst.Impl
{
    using System;
	
    class ByteBuffer
    {
        internal void  extend(int size)
        {
            if (size > arr.Length)
            {
                int newLen = size > arr.Length * 2?size:arr.Length * 2;
                byte[] newArr = new byte[newLen];
                Array.Copy(arr, 0, newArr, 0, used);
                arr = newArr;
            }
            used = size;
        }
		
        internal byte[] toArray()
        {
            byte[] result = new byte[used];
            Array.Copy(arr, 0, result, 0, used);
            return result;
        }
		
        internal ByteBuffer()
        {
            arr = new byte[64];
        }
		
        internal byte[] arr;
        internal int used;
    }
}