package org.garret.perst.impl;

class ByteBuffer {
    final void extend(int size) {  
        if (size > arr.length) { 
            int newLen = size > arr.length*2 ? size : arr.length*2;
            byte[] newArr = new byte[newLen];
            System.arraycopy(arr, 0, newArr, 0, used); 
            arr = newArr;
        }
        used = size;
    }
    
    final byte[] toArray() { 
        byte[] result = new byte[used];
        System.arraycopy(arr, 0, result, 0, used); 
        return result;
    }

    ByteBuffer() { 
        arr = new byte[64];
    }

    byte[] arr;
    int    used;
}




