namespace Perst.Impl
{
    using System;
    using Perst;
	
    //
    // Class for packing/unpacking data
    //
    public class Bytes
    {
        public static short unpack2(byte[] arr, int offs)
        {
            return (short) (((sbyte)arr[offs] << 8) | arr[offs + 1]);
        }

        public static int unpack4(byte[] arr, int offs)
        {
            return ((sbyte)arr[offs] << 24) | (arr[offs + 1] << 16) | (arr[offs + 2] << 8) | arr[offs + 3];
        }

        public static long unpack8(byte[] arr, int offs)
        {
            return ((long) unpack4(arr, offs) << 32) | (uint)unpack4(arr, offs + 4);
        }

        public static float unpackF4(byte[] arr, int offs)
        {
            return BitConverter.ToSingle(BitConverter.GetBytes(unpack4(arr, offs)), 0);
        }

        public static double unpackF8(byte[] arr, int offs)
        {
#if COMPACT_NET_FRAMEWORK 
            return BitConverter.ToDouble(BitConverter.GetBytes(unpack8(arr, offs)), 0);
#else
            return BitConverter.Int64BitsToDouble(unpack8(arr, offs));
#endif
        }

        public static decimal unpackDecimal(byte[] arr, int offs)
        {
            int[] bits = new int[4];
            bits[0] = Bytes.unpack4(arr, offs);
            bits[1] = Bytes.unpack4(arr, offs+4);
            bits[2] = Bytes.unpack4(arr, offs+8);
            bits[3] = Bytes.unpack4(arr, offs+12);
            return new decimal(bits);
        }

        public static int unpackString(byte[] arr, int offs, out string str)
        {
            int len = Bytes.unpack4(arr, offs);
            offs += 4;
            str = null;
            if (len >= 0)
            {
                char[] chars = new char[len];
                for (int i = 0; i < len; i++)
                {
                    chars[i] = (char)Bytes.unpack2(arr, offs);
                    offs += 2;
                }
                str = new string(chars);
            }
            return offs;
        }
    
        public static Guid unpackGuid(byte[] arr, int offs)
        {
            byte[] bits = new byte[16];
            Array.Copy(arr, offs, bits, 0, 16);
            return new Guid(bits);
        }

        public static DateTime unpackDate(byte[] arr, int offs) 
        {
            return new DateTime(unpack8(arr, offs));
        }

        public static void  pack2(byte[] arr, int offs, short val)
        {
            arr[offs] = (byte) (val >> 8);
            arr[offs + 1] = (byte) val;
        }
        public static void  pack4(byte[] arr, int offs, int val)
        {
            arr[offs] = (byte) (val >> 24);
            arr[offs + 1] = (byte) (val >> 16);
            arr[offs + 2] = (byte) (val >> 8);
            arr[offs + 3] = (byte) val;
        }
        public static void  pack8(byte[] arr, int offs, long val)
        {
            pack4(arr, offs, (int) (val >> 32));
            pack4(arr, offs + 4, (int) val);
        }
 
        public static void packF4(byte[] arr, int offs, float val)
        {
            pack4(arr, offs, BitConverter.ToInt32(BitConverter.GetBytes(val), 0));
        }
 
        public static void packF8(byte[] arr, int offs, double val)
        {
#if COMPACT_NET_FRAMEWORK 
            pack8(arr, offs, BitConverter.ToInt64(BitConverter.GetBytes(val), 0));
#else
            pack8(arr, offs, BitConverter.DoubleToInt64Bits(val));
#endif
        }

        public static void packDecimal(byte[] arr, int offs, decimal val)
        {
            int[] bits = Decimal.GetBits(val);
            pack4(arr, offs, bits[0]);
            pack4(arr, offs+4, bits[1]);
            pack4(arr, offs+8, bits[2]);
            pack4(arr, offs+12, bits[3]);
        }

        public static void packGuid(byte[] arr, int offs, Guid val)
        {
            Array.Copy(val.ToByteArray(), 0, arr, offs, 16);
        }

        public static void packDate(byte[] arr, int offs, DateTime val)
        {
            pack8(arr, offs, val.Ticks);
        }
    }
}