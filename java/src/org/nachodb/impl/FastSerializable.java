package org.nachodb.impl;

public interface FastSerializable { 
    int pack(ByteBuffer buf, int offs, String encoding);    
    int unpack(byte[] buf, int offs, String encoding);
}