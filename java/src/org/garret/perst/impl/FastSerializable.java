package org.garret.perst.impl;

public interface FastSerializable { 
    int pack(ByteBuffer buf, int offs);    
    int unpack(byte[] buf, int offs);
}