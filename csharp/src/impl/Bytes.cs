namespace Perst.Impl
{
    using System;
    using Perst;
	
    //
    // Class for packing/unpacking data
    //
    class Bytes
    {
        internal static short unpack2(byte[] arr, int offs)
        {
            return (short) (((sbyte)arr[offs] << 8) | arr[offs + 1]);
        }
        internal static int unpack4(byte[] arr, int offs)
        {
            return ((sbyte)arr[offs] << 24) | (arr[offs + 1] << 16) | (arr[offs + 2] << 8) | arr[offs + 3];
        }
        internal static long unpack8(byte[] arr, int offs)
        {
            return ((long) unpack4(arr, offs) << 32) | (uint)unpack4(arr, offs + 4);
        }
        internal static void  pack2(byte[] arr, int offs, short val)
        {
            arr[offs] = (byte) (val >> 8);
            arr[offs + 1] = (byte) val;
        }
        internal static void  pack4(byte[] arr, int offs, int val)
        {
            arr[offs] = (byte) (val >> 24);
            arr[offs + 1] = (byte) (val >> 16);
            arr[offs + 2] = (byte) (val >> 8);
            arr[offs + 3] = (byte) val;
        }
        internal static void  pack8(byte[] arr, int offs, long val)
        {
            pack4(arr, offs, (int) (val >> 32));
            pack4(arr, offs + 4, (int) val);
        }
    }
}