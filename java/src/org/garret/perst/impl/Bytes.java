package org.garret.perst.impl;
import  org.garret.perst.*;

//
// Class for packing/unpacking data
//
class Bytes {
    static short unpack2(byte[] arr, int offs) { 
	return (short)((arr[offs] << 8) | (arr[offs+1] & 0xFF));
    }
    static int unpack4(byte[] arr, int offs) { 
	return (arr[offs] << 24) | ((arr[offs+1] & 0xFF) << 16)
	    | ((arr[offs+2] & 0xFF) << 8) | (arr[offs+3] & 0xFF);
    }
    static long unpack8(byte[] arr, int offs) { 
	return ((long)unpack4(arr, offs) << 32)
	    | (unpack4(arr, offs+4) & 0xFFFFFFFFL);
    }
    static void pack2(byte[] arr, int offs, short val) { 
	arr[offs] = (byte)(val >> 8);
	arr[offs+1] = (byte)val;
    }
    static void pack4(byte[] arr, int offs, int val) { 
	arr[offs] = (byte)(val >> 24);
	arr[offs+1] = (byte)(val >> 16);
	arr[offs+2] = (byte)(val >> 8);
	arr[offs+3] = (byte)val;
    }
    static void pack8(byte[] arr, int offs, long val) { 
	pack4(arr, offs, (int)(val >> 32));
	pack4(arr, offs+4, (int)val);
    }
}

