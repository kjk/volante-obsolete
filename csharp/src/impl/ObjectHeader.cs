namespace Volante.Impl
{
    using System;
    using Volante;

    class ObjectHeader
    {
        internal const int Sizeof = 8;

        internal static int getSize(byte[] arr, int offs)
        {
            return Bytes.unpack4(arr, offs);
        }
        internal static void setSize(byte[] arr, int offs, int size)
        {
            Bytes.pack4(arr, offs, size);
        }
        internal static int getType(byte[] arr, int offs)
        {
            return Bytes.unpack4(arr, offs + 4);
        }
        internal static void setType(byte[] arr, int offs, int type)
        {
            Bytes.pack4(arr, offs + 4, type);
        }
    }
}