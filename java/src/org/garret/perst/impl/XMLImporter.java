package org.garret.perst.impl;

import org.garret.perst.*;
import java.io.*;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.lang.reflect.Field;

public class XMLImporter { 
    public XMLImporter(StorageImpl storage, Reader reader) { 
        this.storage = storage;
        scanner = new XMLScanner(reader);
    }

    public void importDatabase() throws XMLImportException { 
        if (scanner.scan() != XMLScanner.XML_LT
            || scanner.scan() != XMLScanner.XML_IDENT
            || !scanner.getIdentifier().equals("database"))
        { 
            throwException("No root element");
        }    
        if (scanner.scan() != XMLScanner.XML_IDENT
            || !scanner.getIdentifier().equals("root")
            || scanner.scan() != XMLScanner.XML_EQ
            || scanner.scan() != XMLScanner.XML_SCONST 
            || scanner.scan() != XMLScanner.XML_GT) 
        {
            throwException("Database element should have \"root\" attribute");
        }
        int rootId = 0;
        try { 
            rootId = Integer.parseInt(scanner.getString());
        } catch (NumberFormatException x) { 
            throwException("Incorrect root object specification");
        }
        idMap = new int[rootId*2];
        idMap[rootId] = storage.allocateId();
        storage.header.root[1-storage.currIndex].rootObject = idMap[rootId];

        XMLElement elem;
        int tkn;
        while ((tkn = scanner.scan()) == XMLScanner.XML_LT) { 
            if (scanner.scan() != XMLScanner.XML_IDENT) { 
                throwException("Element name expected");
            }
            String elemName = scanner.getIdentifier();
            if (elemName.equals("btree-index")) { 
                createIndex();
            } else { 
                createObject(readElement(elemName));
            }
        }
        if (tkn != XMLScanner.XML_LTS
            || scanner.scan() != XMLScanner.XML_IDENT
            || !scanner.getIdentifier().equals("database")
            || scanner.scan() != XMLScanner.XML_GT)
        {
            throwException("Root element is not closed");
        }                
    }

    static class XMLElement { 
        private XMLElement next;
        private XMLElement prev;
        private String     name;
        private HashMap    siblings;
        private HashMap    attributes;
        private String     svalue;
        private long       ivalue;
        private double     rvalue;
        private int        valueType;
        private int        counter;

        static final int NO_VALUE     = 0;
        static final int STRING_VALUE = 1;
        static final int INT_VALUE    = 2;
        static final int REAL_VALUE   = 3;
        static final int NULL_VALUE   = 4;

        XMLElement(String name) { 
            this.name = name;
            valueType = NO_VALUE;
        }

        final void addSibling(XMLElement elem) { 
            if (siblings == null) { 
                siblings = new HashMap();
            }
            XMLElement prev = (XMLElement)siblings.put(elem.name, elem);
            if (prev != null) { 
                elem.next = prev.next;
                elem.prev = prev;
                prev.next = elem;
                elem.counter = prev.counter + 1;
            } else { 
                elem.next = elem.prev = elem;
                elem.counter = 1;
            }
        }

        final void addAttribute(String name, String value) { 
            if (attributes == null) { 
                attributes = new HashMap();
            }
            attributes.put(name, value);
        }

        final XMLElement getSibling(String name) { 
            if (siblings != null) { 
                XMLElement sibling = (XMLElement)siblings.get(name);
                if (sibling != null) {
                    return sibling.next;
                }
            }
            return null;
        }
        
        final XMLElement getNextSibling() { 
            return next;
        }

        final int getCounter() { 
            return counter;
        }

        final String getAttribute(String name) { 
            return attributes != null ? (String)attributes.get(name) : null;
        }

        final void setIntValue(long val) { 
            ivalue = val;
            valueType = INT_VALUE;
        }
        
        final void setRealValue(double val) { 
            rvalue = val;
            valueType = REAL_VALUE;
        }
        
        final void setStringValue(String val) { 
            svalue = val;
            valueType = STRING_VALUE;
        }

        final void setNullValue() { 
            valueType = NULL_VALUE;
        }
           
    
        final String getStringValue() { 
            return svalue;
        }

        final long getIntValue() { 
            return ivalue;
        }

        final double getRealValue() { 
            return rvalue;
        }

        final boolean isIntValue() { 
            return valueType == INT_VALUE;
        }
    
        final boolean isRealValue() { 
            return valueType == REAL_VALUE;
        }

        final boolean isStringValue() { 
            return valueType == STRING_VALUE;
        }
    
        final boolean isNullValue() { 
            return valueType == NULL_VALUE;
        }
    }        

    final String getAttribute(XMLElement elem, String name) throws XMLImportException
    { 
        String value = elem.getAttribute(name);
        if (value == null) { 
            throwException("Attribute " + name + " is not set");
        }
        return value;
    }


    final int getIntAttribute(XMLElement elem, String name) throws XMLImportException
    { 
        String value = elem.getAttribute(name);
        if (value == null) { 
            throwException("Attribute " + name + " is not set");
        }
        try { 
            return Integer.parseInt(value);
        } catch (NumberFormatException x) { 
            throwException("Attribute " + name + " should has integer value");
        }
        return -1;
    }

    final int mapId(int id) 
    {
        int oid = 0;
        if (id != 0) {
            if (id >= idMap.length) { 
                int[] newMap = new int[id*2];
                System.arraycopy(idMap, 0, newMap, 0, idMap.length);
                idMap = newMap;
                idMap[id] = oid = storage.allocateId();
            } else { 
                oid = idMap[id];
                if (oid == 0) { 
                    idMap[id] = oid = storage.allocateId();
                }
            }
        }
        return oid;
    }

    final int mapType(String signature) throws XMLImportException
    { 
        for (int i = 0; i < ClassDescriptor.signature.length; i++) { 
            if (ClassDescriptor.signature[i].equals(signature)) { 
                return i;
            }
        }
        throwException("Bad type");
        return -1;
    }

    final Key createKey(int type, String value) throws XMLImportException
    { 
        try { 
            IPersistent obj;
            Date date;
            switch (type) { 
                case ClassDescriptor.tpBoolean:
                    return new Key(Integer.parseInt(value) != 0);
                 case ClassDescriptor.tpByte:
                    return new Key((byte)Integer.parseInt(value));
                case ClassDescriptor.tpChar:
                    return new Key((char)Integer.parseInt(value));
                case ClassDescriptor.tpShort:
                    return new Key((short)Integer.parseInt(value));
                case ClassDescriptor.tpInt:
                    return new Key(Integer.parseInt(value));
                case ClassDescriptor.tpObject:
                    obj = new Persistent();
                    storage.assignOid(obj, mapId(Integer.parseInt(value)));
                    return new Key(obj);
                case ClassDescriptor.tpLong:
                    return new Key(Long.parseLong(value));
                case ClassDescriptor.tpFloat:
                    return new Key(Float.parseFloat(value));
                case ClassDescriptor.tpDouble:
                    return new Key(Double.parseDouble(value));
                case ClassDescriptor.tpString:
                    return new Key(value);
                case ClassDescriptor.tpDate:
                    date = httpFormatter.parse(value, new ParsePosition(0));
                    if (date == null) { 
                        throwException("Invalid date");
                    }               
                    return new Key(date);
                default:
                    throwException("Bad key type");
            }
        } catch (NumberFormatException x) { 
            throwException("Failed to convert key value");
        }
        return null;
    }

    final int parseInt(String str) throws XMLImportException
    {
        try { 
            return Integer.parseInt(str);
        } catch (NumberFormatException x) { 
            throwException("Bad integer constant");
        }
        return -1;
    }

    final void createIndex() throws XMLImportException
    {
        Btree btree;
        int tkn;
        int oid = 0;
        boolean unique = false;
        String className = null;
        String fieldName = null;
        String type = null;
        while ((tkn = scanner.scan()) == XMLScanner.XML_IDENT) { 
            String attrName = scanner.getIdentifier();
            if (scanner.scan() != XMLScanner.XML_EQ || scanner.scan() != XMLScanner.XML_SCONST) {
                throwException("Attribute value expected");
            }
            String attrValue = scanner.getString();
            if (attrName.equals("id")) { 
                oid = mapId(parseInt(attrValue));
            } else if (attrName.equals("unique")) { 
                unique = parseInt(attrValue) != 0;
            } else if (attrName.equals("class")) { 
                className = attrValue;
            } else if (attrName.equals("field")) { 
                fieldName = attrValue;
            } else if (attrName.equals("type")) { 
                type = attrValue;
            }
        }
        if (tkn != XMLScanner.XML_GT) { 
            throwException("Unclosed element tag");
        }
        if (oid == 0) { 
            throwException("ID is not specified or index");
        }
        if (className != null) { 
            if (fieldName == null) { 
                throwException("Field name is not specified for field index");
            }
            Class cls = null;
            try { 
                cls = Class.forName(className);
            } catch (ClassNotFoundException x) { 
                 throwException("Class " + className + " not found");
            }
            btree = new BtreeFieldIndex(cls, fieldName, unique);
        } else { 
            if (type == null) { 
                throwException("Key type is not specified for index");
            }               
            btree = new Btree(mapType(type), unique);
        }
        storage.assignOid(btree, oid);

        while ((tkn = scanner.scan()) == XMLScanner.XML_LT) {
            if (scanner.scan() != XMLScanner.XML_IDENT
                || !scanner.getIdentifier().equals("ref"))
            {
                throwException("<ref> element expected");
            }   
            XMLElement ref = readElement("ref");
            String entryKey = getAttribute(ref, "key");
            int entryOid = mapId(getIntAttribute(ref, "id"));
            Key key = createKey(btree.type, entryKey);
            IPersistent obj = new Persistent();
            storage.assignOid(obj, entryOid);
            btree.insert(key, obj, false);
        }
        if (tkn != XMLScanner.XML_LTS 
            || scanner.scan() != XMLScanner.XML_IDENT
            || !scanner.getIdentifier().equals("btree-index")
            || scanner.scan() != XMLScanner.XML_GT)
        {
            throwException("Element is not closed");
        }           
        byte[] data = storage.packObject(btree);
        int size = ObjectHeader.getSize(data, 0);
        long pos = storage.allocate(size, 0);
        storage.setPos(oid, pos | StorageImpl.dbModifiedFlag);

	storage.pool.put(pos & ~StorageImpl.dbFlagsMask, data, size);
    }

    final void createObject(XMLElement elem) throws XMLImportException
    {
        Class cls = null;
        try { 
            cls = Class.forName(elem.name);
        } catch (ClassNotFoundException x) { 
            throwException("Class " + elem.name + " not found");
        }
        ClassDescriptor desc = storage.getClassDescriptor(cls);
        int oid = mapId(getIntAttribute(elem, "id"));
        ByteBuffer buf = new ByteBuffer();
        int offs = ObjectHeader.sizeof;
        buf.extend(offs);

        offs = packObject(elem, desc, offs, buf);

        ObjectHeader.setSize(buf.arr, 0, offs);
        ObjectHeader.setType(buf.arr, 0, desc.getOid());

        long pos = storage.allocate(offs, 0);
        storage.setPos(oid, pos | StorageImpl.dbModifiedFlag);
	storage.pool.put(pos, buf.arr, offs);
    }

    final int getHexValue(char ch) throws XMLImportException
    { 
        if (ch >= '0' && ch <= '9') { 
            return ch - '0';
        } else if (ch >= 'A' && ch <= 'F') { 
            return ch - 'A' + 10;
        } else if (ch >= 'a' && ch <= 'f') { 
            return ch - 'a' + 10;
        } else { 
            throwException("Bad hexadecimal constant");
        }
        return -1;
    }

    final int packObject(XMLElement objElem, ClassDescriptor desc, int offs, ByteBuffer buf) 
        throws XMLImportException
    { 
        Field[] flds = desc.allFields;
        int[] types = desc.fieldTypes;
        for (int i = 0, n = flds.length; i < n; i++) {
            Field f = flds[i];
            XMLElement elem = (objElem != null) ? objElem.getSibling(f.getName()) : null;
                
            switch(types[i]) {
                case ClassDescriptor.tpByte:
                    buf.extend(offs + 1);
                    if (elem != null) { 
                        if (elem.isIntValue()) {
                            buf.arr[offs] = (byte)elem.getIntValue();
                        } else if (elem.isRealValue()) {
                            buf.arr[offs] = (byte)elem.getRealValue();
                        } else { 
                            throwException("Conversion for field " + f.getName() + " is not possible");
                        }
                    }
                    offs += 1;
                    continue;
                case ClassDescriptor.tpBoolean:
                    buf.extend(offs + 1);
                    if (elem != null) { 
                        if (elem.isIntValue()) {
                            buf.arr[offs] = (byte)(elem.getIntValue() != 0 ? 1 : 0);
                        } else if (elem.isRealValue()) {
                            buf.arr[offs] = (byte)(elem.getRealValue() != 0.0 ? 1 : 0);
                        } else { 
                            throwException("Conversion for field " + f.getName() + " is not possible");
                        }
                    }
                    offs += 1;
                    continue;
                case ClassDescriptor.tpShort:
                case ClassDescriptor.tpChar:
                    buf.extend(offs + 2);
                    if (elem != null) { 
                        if (elem.isIntValue()) {
                            Bytes.pack2(buf.arr, offs, (short)elem.getIntValue());
                        } else if (elem.isRealValue()) {
                            Bytes.pack2(buf.arr, offs, (short)elem.getRealValue());
                        } else { 
                            throwException("Conversion for field " + f.getName() + " is not possible");
                        }
                    }
                    offs += 2;
                    continue;
                case ClassDescriptor.tpInt:
                    buf.extend(offs + 4);
                    if (elem != null) { 
                        if (elem.isIntValue()) {
                            Bytes.pack4(buf.arr, offs, (int)elem.getIntValue());
                        } else if (elem.isRealValue()) {
                            Bytes.pack4(buf.arr, offs, (int)elem.getRealValue());
                        } else { 
                            throwException("Conversion for field " + f.getName() + " is not possible");
                        }
                    }
                    offs += 4;
                    continue;
                case ClassDescriptor.tpLong:
                    buf.extend(offs + 8);
                    if (elem != null) { 
                        if (elem.isIntValue()) {
                            Bytes.pack8(buf.arr, offs, elem.getIntValue());
                        } else if (elem.isRealValue()) {
                            Bytes.pack8(buf.arr, offs, (long)elem.getRealValue());
                        } else { 
                            throwException("Conversion for field " + f.getName() + " is not possible");
                        }
                    }
                    offs += 8;
                    continue;
                case ClassDescriptor.tpFloat:
                    buf.extend(offs + 4);
                    if (elem != null) { 
                        if (elem.isIntValue()) {
                            Bytes.pack4(buf.arr, offs, Float.floatToIntBits((float)elem.getIntValue()));
                        } else if (elem.isRealValue()) {
                            Bytes.pack4(buf.arr, offs, Float.floatToIntBits((float)elem.getRealValue()));
                        } else { 
                            throwException("Conversion for field " + f.getName() + " is not possible");
                        }
                    }
                    offs += 4;
                    continue;
                case ClassDescriptor.tpDouble:
                    buf.extend(offs + 8);
                    if (elem != null) { 
                        if (elem.isIntValue()) {
                            Bytes.pack8(buf.arr, offs, Double.doubleToLongBits((double)elem.getIntValue()));
                        } else if (elem.isRealValue()) {
                            Bytes.pack8(buf.arr, offs, Double.doubleToLongBits((double)elem.getRealValue()));
                        } else { 
                            throwException("Conversion for field " + f.getName() + " is not possible");
                        }
                    }
                    offs += 8;
                    continue;
                case ClassDescriptor.tpDate:
                    buf.extend(offs + 8);
                    if (elem != null) { 
                        if (elem.isIntValue()) {
                            Bytes.pack8(buf.arr, offs, elem.getIntValue());
                        } else if (elem.isNullValue()) {
                            Bytes.pack8(buf.arr, offs, -1);
                        } else if (elem.isStringValue()) {
                            Date date = httpFormatter.parse(elem.getStringValue(), new ParsePosition(0));
                            if (date == null) { 
                                throwException("Invalid date");
                            }
                            Bytes.pack8(buf.arr, offs, date.getTime());
                        } else { 
                            throwException("Conversion for field " + f.getName() + " is not possible");
                        }
                    }
                    offs += 8;
                    continue;
                case ClassDescriptor.tpString:
                    if (elem != null) { 
                        String value = null;
                        if (elem.isIntValue()) {
                            value = Long.toString(elem.getIntValue());
                        } else if (elem.isRealValue()) {
                            value = Double.toString(elem.getRealValue());
                        } else if (elem.isStringValue()) {
                            value = elem.getStringValue();
                        } else if (elem.isNullValue()) {
                            value = null;
                        } else { 
                            throwException("Conversion for field " + f.getName() + " is not possible");
                        }
                        if (value != null) { 
                            int len = value.length();
                            buf.extend(offs + 4 + len*2);
                            Bytes.pack4(buf.arr, offs, len);
                            offs += 4;
                            for (int j = 0; j < len; j++) { 
                                Bytes.pack2(buf.arr, offs, (short)value.charAt(j));
                                offs += 2;
                            }
                            continue;
                        }
                    } 
                    buf.extend(offs + 4);
                    Bytes.pack4(buf.arr, offs, -1);
                    offs += 4;
                    continue;
                case ClassDescriptor.tpObject:
                {
                    int oid = 0;
                    if (elem != null) {
                        XMLElement ref = elem.getSibling("ref");
                        if (ref == null) { 
                            throwException("<ref> element expected");
                        }
                        oid = mapId(getIntAttribute(ref, "id"));
                    }
                    buf.extend(offs + 4);
                    Bytes.pack4(buf.arr, offs, oid);
                    offs += 4;
                    continue;
                }
                case ClassDescriptor.tpValue:
                    offs = packObject(elem, storage.getClassDescriptor(f.getType()), offs, buf);
                    continue;
                case ClassDescriptor.tpArrayOfByte:
                    if (elem == null || elem.isNullValue()) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else if (elem.isStringValue()) {
                        String hexStr = elem.getStringValue();
                        int len = hexStr.length();
                        buf.extend(offs + 4 + len/2);
                        Bytes.pack4(buf.arr, offs, len/2);
                        offs += 4;
                        for (int j = 0; j < len; j += 2) { 
                            buf.arr[offs++] = (byte)((getHexValue(hexStr.charAt(j)) << 4) | getHexValue(hexStr.charAt(j+1)));
                        }
                    } else { 
                        XMLElement item = elem.getSibling("array-element");
                        int len = (item == null) ? 0 : item.getCounter(); 
                        buf.extend(offs + 4 + len);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        while (--len >= 0) { 
                            if (item.isIntValue()) { 
                                buf.arr[offs] = (byte)item.getIntValue();
                            } else if (item.isRealValue()) { 
                                buf.arr[offs] = (byte)item.getRealValue();
                            } else {
                                throwException("Conversion for field " + f.getName() + " is not possible");
                            }
                            item = item.getNextSibling();
                            offs += 1;
                        }
                    }
                    continue;
                case ClassDescriptor.tpArrayOfBoolean:
                    if (elem == null || elem.isNullValue()) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        XMLElement item = elem.getSibling("array-element");
                        int len = (item == null) ? 0 : item.getCounter(); 
                        buf.extend(offs + 4 + len);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        while (--len >= 0) { 
                            if (item.isIntValue()) { 
                                buf.arr[offs] = (byte)(item.getIntValue() != 0 ? 1 : 0);
                            } else if (item.isRealValue()) { 
                                buf.arr[offs] = (byte)(item.getRealValue() != 0.0 ? 1 : 0);
                            } else {
                                throwException("Conversion for field " + f.getName() + " is not possible");
                            }
                            item = item.getNextSibling();
                            offs += 1;
                        }
                    }
                    continue;
                case ClassDescriptor.tpArrayOfChar:
                case ClassDescriptor.tpArrayOfShort:
                    if (elem == null || elem.isNullValue()) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        XMLElement item = elem.getSibling("array-element");
                        int len = (item == null) ? 0 : item.getCounter(); 
                        buf.extend(offs + 4 + len*2);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        while (--len >= 0) { 
                            if (item.isIntValue()) { 
                                Bytes.pack2(buf.arr, offs, (short)item.getIntValue());
                            } else if (item.isRealValue()) { 
                                Bytes.pack2(buf.arr, offs, (short)item.getRealValue());
                            } else {
                                throwException("Conversion for field " + f.getName() + " is not possible");
                            }
                            item = item.getNextSibling();
                            offs += 2;
                        }
                    }
                    continue;
                case ClassDescriptor.tpArrayOfInt:
                    if (elem == null || elem.isNullValue()) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        XMLElement item = elem.getSibling("array-element");
                        int len = (item == null) ? 0 : item.getCounter(); 
                        buf.extend(offs + 4 + len*4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        while (--len >= 0) { 
                            if (item.isIntValue()) { 
                                Bytes.pack4(buf.arr, offs, (int)item.getIntValue());
                            } else if (item.isRealValue()) { 
                                Bytes.pack4(buf.arr, offs, (int)item.getRealValue());
                            } else {
                                throwException("Conversion for field " + f.getName() + " is not possible");
                            }
                            item = item.getNextSibling();
                            offs += 4;
                        }
                    }
                    continue;
                case ClassDescriptor.tpArrayOfLong:
                    if (elem == null || elem.isNullValue()) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        XMLElement item = elem.getSibling("array-element");
                        int len = (item == null) ? 0 : item.getCounter(); 
                        buf.extend(offs + 4 + len*8);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        while (--len >= 0) { 
                            if (item.isIntValue()) { 
                                Bytes.pack8(buf.arr, offs, item.getIntValue());
                            } else if (item.isRealValue()) { 
                                Bytes.pack8(buf.arr, offs, (long)item.getRealValue());
                            } else {
                                throwException("Conversion for field " + f.getName() + " is not possible");
                            }
                            item = item.getNextSibling();
                            offs += 8;
                        }
                    }
                    continue;
                case ClassDescriptor.tpArrayOfFloat:
                    if (elem == null || elem.isNullValue()) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        XMLElement item = elem.getSibling("array-element");
                        int len = (item == null) ? 0 : item.getCounter(); 
                        buf.extend(offs + 4 + len*4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        while (--len >= 0) { 
                            if (item.isIntValue()) { 
                                Bytes.pack4(buf.arr, offs, Float.floatToIntBits((float)item.getIntValue()));
                            } else if (item.isRealValue()) { 
                                Bytes.pack4(buf.arr, offs, Float.floatToIntBits((float)item.getRealValue()));
                            } else {
                                throwException("Conversion for field " + f.getName() + " is not possible");
                            }
                            item = item.getNextSibling();
                            offs += 4;
                        }
                    }
                    continue;
                case ClassDescriptor.tpArrayOfDouble:
                    if (elem == null || elem.isNullValue()) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        XMLElement item = elem.getSibling("array-element");
                        int len = (item == null) ? 0 : item.getCounter(); 
                        buf.extend(offs + 4 + len*8);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        while (--len >= 0) { 
                            if (item.isIntValue()) { 
                                Bytes.pack8(buf.arr, offs, Double.doubleToLongBits((double)item.getIntValue()));
                            } else if (item.isRealValue()) { 
                                Bytes.pack8(buf.arr, offs, Double.doubleToLongBits((double)item.getRealValue()));
                            } else {
                                throwException("Conversion for field " + f.getName() + " is not possible");
                            }
                            item = item.getNextSibling();
                            offs += 8;
                        }
                    }
                    continue;
                case ClassDescriptor.tpArrayOfDate:
                    if (elem == null || elem.isNullValue()) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        XMLElement item = elem.getSibling("array-element");
                        int len = (item == null) ? 0 : item.getCounter(); 
                        buf.extend(offs + 4 + len*8);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        while (--len >= 0) { 
                            if (item.isNullValue()) { 
                                Bytes.pack8(buf.arr, offs, -1);
                            } else if (item.isStringValue()) { 
                                Date date = httpFormatter.parse(item.getStringValue(), new ParsePosition(0));
                                if (date == null) { 
                                    throwException("Invalid date");
                                }
                                Bytes.pack8(buf.arr, offs, date.getTime());
                            } else {
                                throwException("Conversion for field " + f.getName() + " is not possible");
                            }
                            item = item.getNextSibling();
                            offs += 8;
                        }
                    }
                    continue;
                case ClassDescriptor.tpArrayOfString:
                    if (elem == null || elem.isNullValue()) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        XMLElement item = elem.getSibling("array-element");
                        int len = (item == null) ? 0 : item.getCounter(); 
                        buf.extend(offs + 4 + len*4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        while (--len >= 0) { 
                            String value = null;
                            if (item.isIntValue()) {
                                value = Long.toString(item.getIntValue());
                            } else if (item.isRealValue()) {
                                value = Double.toString(item.getRealValue());
                            } else if (item.isStringValue()) {
                                value = item.getStringValue();
                            } else if (elem.isNullValue()) {
                                value = null;
                            } else { 
                                throwException("Conversion for field " + f.getName() + " is not possible");
                            }
                            if (value == null) { 
                                buf.extend(offs + 4);
                                Bytes.pack4(buf.arr, offs, -1);
                                offs += 4;
                            } else { 
                                int strlen = value.length();
                                buf.extend(offs + 4 + len*2);
                                Bytes.pack4(buf.arr, offs, len);
                                offs += 4;
                                for (int j = 0; j < strlen; j++) { 
                                    Bytes.pack2(buf.arr, offs, (short)value.charAt(j));
                                    offs += 2;
                                }
                            }
                            item = item.getNextSibling();
                        }
                    }
                    continue;
                case ClassDescriptor.tpArrayOfObject:
                case ClassDescriptor.tpLink:
                    if (elem == null || elem.isNullValue()) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        XMLElement item = elem.getSibling("array-element");
                        int len = (item == null) ? 0 : item.getCounter(); 
                        buf.extend(offs + 4 + len*4);
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        while (--len >= 0) { 
                            XMLElement ref = item.getSibling("ref");
                            if (ref == null) { 
                                throwException("<ref> element expected");
                            }
                            int oid = mapId(getIntAttribute(ref, "id"));
                            Bytes.pack4(buf.arr, offs, oid);
                            item = item.getNextSibling();
                            offs += 4;
                        }
                    }
                    continue;
                case ClassDescriptor.tpArrayOfValue:
                    if (elem == null || elem.isNullValue()) {
                        buf.extend(offs + 4);
                        Bytes.pack4(buf.arr, offs, -1);
                        offs += 4;
                    } else {
                        XMLElement item = elem.getSibling("array-element");
                        int len = (item == null) ? 0 : item.getCounter(); 
                        Bytes.pack4(buf.arr, offs, len);
                        offs += 4;
                        ClassDescriptor elemDesc = storage.getClassDescriptor(f.getType());
                        while (--len >= 0) {
                            offs = packObject(item, elemDesc, offs, buf);
                            item = item.getNextSibling();
                        }
                    }
                    continue;
            }
        }
        return offs;
    }

    final XMLElement readElement(String name) throws XMLImportException 
    {
        XMLElement elem = new XMLElement(name);
        String attribute;
        int tkn;
        while (true) { 
            switch (scanner.scan()) { 
              case XMLScanner.XML_GTS:
                return elem;
              case XMLScanner.XML_GT:    
                while ((tkn = scanner.scan()) == XMLScanner.XML_LT) { 
                    if (scanner.scan() != XMLScanner.XML_IDENT) { 
                        throwException("Element name expected");
                    }
                    String siblingName = scanner.getIdentifier();
                    XMLElement sibling = readElement(siblingName);
                    elem.addSibling(sibling);
                }
                switch (tkn) { 
                  case XMLScanner.XML_SCONST:
                    elem.setStringValue(scanner.getString());
                    tkn = scanner.scan();
                    break;
                  case XMLScanner.XML_ICONST:
                    elem.setIntValue(scanner.getInt());
                    tkn = scanner.scan();
                    break;
                  case XMLScanner.XML_FCONST:
                    elem.setRealValue(scanner.getReal());
                    tkn = scanner.scan();
                    break;
                  case XMLScanner.XML_IDENT:
                    if (scanner.getIdentifier().equals("null")) { 
                        elem.setNullValue();
                        tkn = scanner.scan();
                    } else { 
                        throwException("null expected");
                    }
                }
                if (tkn != XMLScanner.XML_LTS                    
                    || scanner.scan() != XMLScanner.XML_IDENT
                    || !scanner.getIdentifier().equals(name)
                    || scanner.scan() != XMLScanner.XML_GT)
                {
                    throwException("Element is not closed");
                }
                return elem;
              case XMLScanner.XML_IDENT:
                attribute = scanner.getIdentifier();
                if (scanner.scan() != XMLScanner.XML_EQ || scanner.scan() != XMLScanner.XML_SCONST)
                {
                    throwException("Attribute value expected");
                }
                elem.addAttribute(attribute, scanner.getString());
                continue;
              default:
                throwException("Unexpected token");
            }
        }
    }

    final void throwException(String message) throws XMLImportException { 
        throw new XMLImportException(scanner.getLine(), scanner.getColumn(), message);
    }

    StorageImpl storage;
    XMLScanner  scanner;
    int[]       idMap;

    static final String     dateFormat = "EEE, d MMM yyyy kk:mm:ss z";
    static final DateFormat httpFormatter = new SimpleDateFormat(dateFormat, Locale.ENGLISH);


    static class XMLScanner { 
        static final int XML_IDENT  = 0;
        static final int XML_SCONST = 1;
        static final int XML_ICONST = 2; 
        static final int XML_FCONST = 3; 
        static final int XML_LT     = 4; 
        static final int XML_GT     = 5; 
        static final int XML_LTS    = 6; 
        static final int XML_GTS    = 7;
        static final int XML_EQ     = 8; 
        static final int XML_EOF    = 9;
        
        Reader         reader;
        int            line;
        int            column;
        char[]         sconst;
        long           iconst;
        double         fconst;
        int            slen;
        String         ident;
        int            size;
        int            ungetChar;
        boolean        hasUngetChar;

        XMLScanner(Reader in) {
            reader = in;
            sconst = new char[size = 1024];
            line = 1;
            column = 0;
            hasUngetChar = false;
        }

        final int get() throws XMLImportException { 
            if (hasUngetChar) { 
                hasUngetChar = false;
                return ungetChar;
            }
            try { 
                int ch = reader.read();
                if (ch == '\n') { 
                    line += 1;
                    column = 0;
                } else if (ch == '\t') { 
                    column += (column + 8) & ~7;
                } else { 
                    column += 1;
                }
                return ch;
            } catch (IOException x) { 
                throw new XMLImportException(line, column, x.getMessage());
            }
        }
        
        final void unget(int ch) { 
            if (ch == '\n') {
                line -= 1;
            } else { 
                column -= 1;
            }
            ungetChar = ch;
            hasUngetChar = true;
        }
        
        final int scan() throws XMLImportException
        {
            int i, ch;
            boolean floatingPoint;

            while (true) { 
                do {
                    if ((ch = get()) < 0) {
                        return XML_EOF;
                    }
                } while (ch <= ' ');
                
                switch (ch) { 
                  case '<':
                    ch = get();
                    if (ch == '?') { 
                        while ((ch = get()) != '?') { 
                            if (ch < 0) { 
                                throw new XMLImportException(line, column, "Bad XML file format");
                            }
                        }
                        if ((ch = get()) != '>') { 
                            throw new XMLImportException(line, column, "Bad XML file format");
                        }
                        continue;
                    } 
                    if (ch != '/') { 
                        unget(ch);
                        return XML_LT;
                    }
                    return XML_LTS;
                  case '>':
                    return XML_GT;
                  case '/':
                    ch = get();
                    if (ch != '>') { 
                        unget(ch);
                        throw new XMLImportException(line, column, "Bad XML file format");
                    }
                    return XML_GTS;
                  case '=':
                    return XML_EQ;
                  case '"':
                    i = 0;
                    while (true) { 
                        ch = get();
                        if (ch < 0) { 
                            throw new XMLImportException(line, column, "Bad XML file format");
                        } else if (ch == '&') { 
                            switch (get()) { 
                              case 'a':
                                if (get() != 'm' || get() != 'p' || get() != ';') { 
                                    throw new XMLImportException(line, column, "Bad XML file format");
                                }
                                ch = '&';
                                break;
                              case 'l':
                                if (get() != 't' || get() != ';') { 
                                    throw new XMLImportException(line, column, "Bad XML file format");
                                }
                                ch = '<';
                                break;
                              case 'g':
                                if (get() != 't' || get() != ';') { 
                                    throw new XMLImportException(line, column, "Bad XML file format");
                                }
                                ch = '>';
                                break;
                              case 'q':
                                if (get() != 'u' || get() != 'o' || get() != 't' || get() != ';') { 
                                    throw new XMLImportException(line, column, "Bad XML file format");
                                }
                                ch = '"';
                                break;
                              default: 
                                throw new XMLImportException(line, column, "Bad XML file format");
                            }
                        } else if (ch == '"') { 
                            slen = i;
                            return XML_SCONST;
                        }
                        if (i == size) { 
                            char[] newBuf = new char[size *= 2];
                            System.arraycopy(sconst, 0, newBuf, 0, slen);
                            sconst = newBuf;
                        }
                        sconst[i++] = (char)ch;
                    }
                  case '-': case '+':
                  case '0': case '1': case '2': case '3': case '4':
                  case '5': case '6': case '7': case '8': case '9':
                    i = 0;
                    floatingPoint = false;
                    while (true) { 
                        if (!Character.isDigit((char)ch) && ch != '-' && ch != '+' && ch != '.' && ch != 'E') { 
                            unget(ch);
                            try { 
                                if (floatingPoint) { 
                                    fconst = Double.parseDouble(new String(sconst, 0, i));
                                    return XML_FCONST;
                                } else { 
                                    iconst = Long.parseLong(new String(sconst, 0, i));
                                    return XML_ICONST;
                                }
                            } catch (NumberFormatException x) { 
                                throw new XMLImportException(line, column, "Bad XML file format");
                            }
                        }
                        if (i == size) { 
                            throw new XMLImportException(line, column, "Bad XML file format");
                        }
                        sconst[i++] = (char)ch;
                        if (ch == '.') { 
                            floatingPoint = true;
                        }
                        ch = get();
                    }
                  default:
                    i = 0;
                    while (Character.isLetterOrDigit((char)ch) || ch == '$' || ch == '-' || ch == ':' || ch == '_' || ch == '.') {
                        if (i == size) { 
                            throw new XMLImportException(line, column, "Bad XML file format");
                        }
                        sconst[i++] = (char)ch;
                        ch = get();
                    }
                    unget(ch);
                    if (i == 0) { 
                        throw new XMLImportException(line, column, "Bad XML file format");
                    }
                    ident = new String(sconst, 0, i);
                    return XML_IDENT;
                }
            }
        }
        
        final String getIdentifier() { 
            return ident;
        }

        final String getString() { 
            return new String(sconst, 0, slen);
        }

        final long getInt() { 
            return iconst;
        }

        final double getReal() { 
            return fconst;
        }

        final int    getLine() { 
            return line;
        }

        final int    getColumn() { 
            return column;
        }
    }
}
