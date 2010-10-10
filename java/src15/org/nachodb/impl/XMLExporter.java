package org.nachodb.impl;

import java.io.*;
import java.util.Date;
import java.lang.reflect.Field;
import org.nachodb.Assert;

public class XMLExporter { 
    public XMLExporter(StorageImpl storage, Writer writer) { 
        this.storage = storage;
        this.writer = writer;
    }

    public void exportDatabase(int rootOid) throws IOException 
    { 
        writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        writer.write("<database root=\"" + rootOid + "\">\n");
        exportedBitmap = new int[(storage.currIndexSize + 31) / 32];
        markedBitmap = new int[(storage.currIndexSize + 31) / 32];
        markedBitmap[rootOid >> 5] |= 1 << (rootOid & 31);
        int nExportedObjects;
        do { 
            nExportedObjects = 0;
            for (int i = 0; i < markedBitmap.length; i++) { 
                int mask = markedBitmap[i];
                if (mask != 0) { 
                    for (int j = 0, bit = 1; j < 32; j++, bit <<= 1) { 
                        if ((mask & bit) != 0) { 
                            int oid = (i << 5) + j;
                            exportedBitmap[i] |= bit;
                            markedBitmap[i] &= ~bit;
                            byte[] obj = storage.get(oid);
                            int typeOid = ObjectHeader.getType(obj, 0);                
                            ClassDescriptor desc = storage.findClassDescriptor(typeOid);
                            if (desc.cls == Btree.class) { 
                                exportIndex(oid, obj, "org.nachodb.impl.Btree");
                            } else if (desc.cls == BitIndexImpl.class) { 
                                exportIndex(oid, obj, "org.nachodb.impl.BitIndexImpl");
                            } else if (desc.cls == PersistentSet.class) { 
                                exportSet(oid, obj);
                            } else if (desc.cls == BtreeFieldIndex.class) { 
                                exportFieldIndex(oid, obj);
                            } else if (desc.cls == BtreeMultiFieldIndex.class) { 
                                exportMultiFieldIndex(oid, obj);
                            } else { 
                                String className = exportIdentifier(desc.name);
                                writer.write(" <" + className + " id=\"" + oid + "\">\n");
                                exportObject(desc, obj, ObjectHeader.sizeof, 2);
                                writer.write(" </" + className + ">\n");
                            }
                            nExportedObjects += 1;
                        }
                    }
                }
            }                            
        } while (nExportedObjects != 0);
        writer.write("</database>\n");   
        writer.flush(); // writer should be closed by calling code
    }

    final String exportIdentifier(String name) { 
        return name.replace('$', '-');
    }

    final void exportSet(int oid,  byte[] data) throws IOException 
    { 
        Btree btree = new Btree(data, ObjectHeader.sizeof);
        storage.assignOid(btree, oid);
        writer.write(" <org.nachodb.impl.PersistentSet id=\"" + oid + "\">\n");
        btree.export(this);
        writer.write(" </org.nachodb.impl.PersistentSet>\n");
    }

    final void exportIndex(int oid, byte[] data, String name) throws IOException 
    { 
        Btree btree = new Btree(data, ObjectHeader.sizeof);
        storage.assignOid(btree, oid);
        writer.write(" <" + name + " id=\"" + oid + "\" unique=\"" + (btree.unique ? '1' : '0') 
                     + "\" type=\"" + ClassDescriptor.signature[btree.type] + "\">\n");
        btree.export(this);
        writer.write(" </" + name + ">\n");
    }

    final void exportFieldIndex(int oid,  byte[] data) throws IOException
    { 
        Btree btree = new Btree(data, ObjectHeader.sizeof);
        storage.assignOid(btree, oid);
        writer.write(" <org.nachodb.impl.BtreeFieldIndex id=\"" + oid + "\" unique=\"" + (btree.unique ? '1' : '0')
                     + "\" class=");
        int offs = exportString(data, Btree.sizeof);
        writer.write(" field=");
        offs = exportString(data, offs);
        writer.write(" autoinc=\"" + Bytes.unpack8(data, offs) + "\">\n");
        btree.export(this);
        writer.write(" </org.nachodb.impl.BtreeFieldIndex>\n");
    }

    final void exportMultiFieldIndex(int oid,  byte[] data) throws IOException
    { 
        Btree btree = new Btree(data, ObjectHeader.sizeof);
        storage.assignOid(btree, oid);
        writer.write(" <org.nachodb.impl.BtreeMultiFieldIndex id=\"" + oid + "\" unique=\"" + (btree.unique ? '1' : '0')
                     + "\" class=");
        int offs = exportString(data, Btree.sizeof);
        int nFields = Bytes.unpack4(data, offs);
        offs += 4;
        for (int i = 0; i < nFields; i++) { 
            writer.write(" field" + i + "=");
            offs = exportString(data, offs);
        }
        writer.write(">\n");
        int nTypes = Bytes.unpack4(data, offs);
        offs += 4;
        compoundKeyTypes = new int[nTypes];
        for (int i = 0; i < nTypes; i++) { 
            compoundKeyTypes[i] = Bytes.unpack4(data, offs);
            offs += 4;
        }
        btree.export(this); 
        compoundKeyTypes = null;
        writer.write(" </org.nachodb.impl.BtreeMultiFieldIndex>\n");
    }

    final int exportKey(byte[] body, int offs, int size, int type) throws IOException
    {
        switch (type) { 
            case ClassDescriptor.tpBoolean:
                writer.write(body[offs++] != 0 ? "1" : "0");
                break;
            case ClassDescriptor.tpByte:
                writer.write(Integer.toString(body[offs++]));
                break;
            case ClassDescriptor.tpChar:
                writer.write(Integer.toString((char)Bytes.unpack2(body, offs)));
                offs += 2;
                break;
            case ClassDescriptor.tpShort:
                writer.write(Integer.toString(Bytes.unpack2(body, offs)));
                offs += 2;
                break;
            case ClassDescriptor.tpInt:
            case ClassDescriptor.tpObject:
            case ClassDescriptor.tpEnum:
                writer.write(Integer.toString(Bytes.unpack4(body, offs)));
                offs += 4;
                break;
            case ClassDescriptor.tpLong:
                writer.write(Long.toString(Bytes.unpack8(body, offs)));
                offs += 8;
                break;
            case ClassDescriptor.tpFloat:
                writer.write(Float.toString(Float.intBitsToFloat(Bytes.unpack4(body, offs))));
                offs += 4;
                break;
            case ClassDescriptor.tpDouble:
                writer.write(Double.toString(Double.longBitsToDouble(Bytes.unpack8(body, offs))));
                offs += 8;
                break;
            case ClassDescriptor.tpString:
                for (int i = 0; i < size; i++) { 
                    exportChar((char)Bytes.unpack2(body, offs));
                    offs += 2;
                }
                break;
            case ClassDescriptor.tpArrayOfByte:
                for (int i = 0; i < size; i++) { 
                    byte b = body[offs++];
                    writer.write(hexDigit[(b >>> 4) & 0xF]);
                    writer.write(hexDigit[b & 0xF]);
                }
                break;
            case ClassDescriptor.tpDate:
            {
                long msec = Bytes.unpack8(body, offs);
                offs += 8;
                if (msec >= 0) { 
                    writer.write(XMLImporter.httpFormatter.format(new Date(msec)));
                } else { 
                    writer.write("null");
                }
                break;
            }
            default:
                Assert.that(false);
        }
        return offs;
    }

    final void exportCompoundKey(byte[] body, int offs, int size, int type) throws IOException 
    { 
        Assert.that(type == ClassDescriptor.tpArrayOfByte);
        int end = offs + size;
        for (int i = 0; i < compoundKeyTypes.length; i++) { 
            type = compoundKeyTypes[i];
            if (type == ClassDescriptor.tpArrayOfByte || type == ClassDescriptor.tpString) { 
                size = Bytes.unpack4(body, offs);
                offs += 4;
            }
            writer.write(" key" + i + "=\"");
            offs = exportKey(body, offs, size, type); 
            writer.write("\"");
        }
        Assert.that(offs == end);
    }

    final void exportAssoc(int oid, byte[] body, int offs, int size, int type) throws IOException
    {
        writer.write("  <ref id=\"" + oid + "\"");
        if ((exportedBitmap[oid >> 5] & (1 << (oid & 31))) == 0) { 
            markedBitmap[oid >> 5] |= 1 << (oid & 31);
        }
        if (compoundKeyTypes != null) { 
            exportCompoundKey(body, offs, size, type);
        } else { 
            writer.write(" key=\"");
            exportKey(body, offs, size, type);
            writer.write("\"");
        }
        writer.write("/>\n");
    }

    final void indentation(int indent) throws IOException { 
        while (--indent >= 0) { 
            writer.write(' ');
        }
    }

    final void exportChar(char ch) throws IOException { 
        switch (ch) {
          case '<':
            writer.write("&lt;");
            break;
          case '>':
            writer.write("&gt;");
            break;
          case '&':
            writer.write("&amp;");
            break;
          case '"':
            writer.write("&quot;");
            break;
          default:
            writer.write(ch);
        }
    }

    final int exportString(byte[] body, int offs) throws IOException { 
        int len = Bytes.unpack4(body, offs);
        offs += 4;
        if (len >= 0) { 
            writer.write("\"");                    
            while (--len >= 0) { 
                exportChar((char)Bytes.unpack2(body, offs));
                offs += 2;
            }
            writer.write("\"");                    
        } else if (len < -1) { 
            writer.write("\"");   
            String s;
            if (storage.encoding != null) { 
                s = new String(body, offs, -len-2, storage.encoding);
            } else { 
                s = new String(body, offs, -len-2);
            }
            offs -= len+2;
            for (int i = 0, n = s.length(); i < n; i++) { 
                exportChar(s.charAt(i));
            }
        } else { 
            writer.write("null");
        }       
        return offs;
    }

    static final char hexDigit[] = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    final int exportBinary(byte[] body, int offs) throws IOException { 
        int len = Bytes.unpack4(body, offs);
        offs += 4;
        if (len < 0) { 
            if (len == -2-ClassDescriptor.tpObject) { 
                exportRef(Bytes.unpack4(body, offs));
                offs += 4;
            } else if (len < -1) { 
                writer.write("\"#");
                writer.write(hexDigit[-2-len]);
                len = ClassDescriptor.sizeof[-2-len];
                while (--len >= 0) {
                    byte b = body[offs++];
                    writer.write(hexDigit[(b >>> 4) & 0xF]);
                    writer.write(hexDigit[b & 0xF]);
                }
                writer.write('\"');
            } else { 
                writer.write("null");
            }
        } else {
            writer.write('\"');
            while (--len >= 0) {
                byte b = body[offs++];
                writer.write(hexDigit[(b >>> 4) & 0xF]);
                writer.write(hexDigit[b & 0xF]);
            }
            writer.write('\"');
        }
        return offs;
    }
    
    final void exportRef(int oid) throws IOException { 
        writer.write("<ref id=\"" + oid + "\"/>");
        if (oid != 0 && (exportedBitmap[oid >> 5] & (1 << (oid & 31))) == 0) { 
            markedBitmap[oid >> 5] |= 1 << (oid & 31);
        }
    }

    final int exportObject(ClassDescriptor desc, byte[] body, int offs, int indent) throws IOException {
        ClassDescriptor.FieldDescriptor[] all = desc.allFields;

        for (int i = 0, n = all.length; i < n; i++) { 
            ClassDescriptor.FieldDescriptor fd = all[i];
            indentation(indent);
            String fieldName = exportIdentifier(fd.fieldName);
            writer.write("<" + fieldName + ">");
            switch (fd.type) { 
                case ClassDescriptor.tpBoolean:
                    writer.write(body[offs++] != 0 ? "1" : "0");
                    break;
                case ClassDescriptor.tpByte:
                    writer.write(Integer.toString(body[offs++]));
                    break;
                case ClassDescriptor.tpChar:
                    writer.write(Integer.toString((char)Bytes.unpack2(body, offs)));
                    offs += 2;
                    break;
                case ClassDescriptor.tpShort:
                    writer.write(Integer.toString(Bytes.unpack2(body, offs)));
                    offs += 2;
                    break;
                case ClassDescriptor.tpInt:
                    writer.write(Integer.toString(Bytes.unpack4(body, offs)));
                    offs += 4;
                    break;
                case ClassDescriptor.tpLong:
                    writer.write(Long.toString(Bytes.unpack8(body, offs)));
                    offs += 8;
                    break;
                case ClassDescriptor.tpFloat:
                    writer.write(Float.toString(Float.intBitsToFloat(Bytes.unpack4(body, offs))));
                    offs += 4;
                    break;
                case ClassDescriptor.tpDouble:
                    writer.write(Double.toString(Double.longBitsToDouble(Bytes.unpack8(body, offs))));
                    offs += 8;
                    break;
                case ClassDescriptor.tpEnum:
                    writer.write("\"" + ((Enum)fd.field.getType().getEnumConstants()[Bytes.unpack4(body, offs)]).name() + "\"");
                    offs += 4;
                    break;
                case ClassDescriptor.tpString:
                    offs = exportString(body, offs);
                    break;
                case ClassDescriptor.tpDate:
                {
                    long msec = Bytes.unpack8(body, offs);
                    offs += 8;
                    if (msec >= 0) { 
                        writer.write("\"" + XMLImporter.httpFormatter.format(new Date(msec)) + "\"");
                    } else { 
                        writer.write("null");
                    }
                    break;
                }
                case ClassDescriptor.tpObject:
                    exportRef(Bytes.unpack4(body, offs));
                    offs += 4;
                    break;
                case ClassDescriptor.tpValue:
                    writer.write('\n');
                    offs = exportObject(fd.valueDesc, body, offs, indent+1);
                    indentation(indent);
                    break;
                case ClassDescriptor.tpRaw:
                case ClassDescriptor.tpArrayOfByte:
                    offs = exportBinary(body, offs);
                    break;
                case ClassDescriptor.tpArrayOfBoolean:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) { 
                            indentation(indent+1);
                            writer.write("<element>" + (body[offs++] != 0 ? "1" : "0") + "</element>\n");
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfChar:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) { 
                            indentation(indent+1);
                            writer.write("<element>" + (Bytes.unpack2(body, offs) & 0xFFFF) + "</element>\n");
                            offs += 2;
                        }
                        indentation(indent);
                    }
                    break;
                }
                 case ClassDescriptor.tpArrayOfShort:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) { 
                            indentation(indent+1);
                            writer.write("<element>" + Bytes.unpack2(body, offs) + "</element>\n");
                            offs += 2;
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfInt:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        Enum[] enumConstants = (Enum[])fd.field.getType().getEnumConstants();
                        while (--len >= 0) { 
                            indentation(indent+1);
                            writer.write("<element>\"" + enumConstants[Bytes.unpack4(body, offs)].name() + "\"</element>\n");
                            offs += 4;
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfLong:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) { 
                            indentation(indent+1);
                            writer.write("<element>" + Bytes.unpack8(body, offs) + "</element>\n");
                            offs += 8;
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfFloat:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) { 
                            indentation(indent+1);
                            writer.write("<element>" 
                                         + Float.intBitsToFloat(Bytes.unpack4(body, offs)) 
                                         + "</element>\n");
                            offs += 4;
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfDouble:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) { 
                            indentation(indent+1);
                            writer.write("<element>" 
                                         + Double.longBitsToDouble(Bytes.unpack8(body, offs)) 
                                         + "</element>\n");
                            offs += 8;
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfDate:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) { 
                            indentation(indent+1);
                            long msec = Bytes.unpack8(body, offs);
                            offs += 8;
                            if (msec >= 0) { 
                                writer.write("<element>\"");
                                writer.write(XMLImporter.httpFormatter.format(new Date(msec)));
                                writer.write("\"</element>\n");
                            } else { 
                                writer.write("<element>null</element>\n");
                            }
                        }
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfString:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) { 
                            indentation(indent+1);
                            writer.write("<element>");
                            offs = exportString(body, offs);
                            writer.write("</element>\n");
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpLink:
                case ClassDescriptor.tpArrayOfObject:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) { 
                            indentation(indent+1);
                            int oid = Bytes.unpack4(body, offs);
                            if (oid != 0 && (exportedBitmap[oid >> 5] & (1 << (oid & 31))) == 0) { 
                                markedBitmap[oid >> 5] |= 1 << (oid & 31);
                            }
                            writer.write("<element><ref id=\"" + oid + "\"/></element>\n");
                            offs += 4;
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfValue:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) { 
                            indentation(indent+1);
                            writer.write("<element>\n");
                            offs = exportObject(fd.valueDesc, body, offs, indent+2);
                            indentation(indent+1);
                            writer.write("</element>\n");
                        }
                        indentation(indent);
                    }
                    break;
                }
                case ClassDescriptor.tpArrayOfRaw:
                {
                    int len = Bytes.unpack4(body, offs);
                    offs += 4;
                    if (len < 0) { 
                        writer.write("null");
                    } else {
                        writer.write('\n');
                        while (--len >= 0) { 
                            indentation(indent+1);
                            writer.write("<element>");
                            offs = exportBinary(body, offs);
                            writer.write("</element>\n");
                        }
                        indentation(indent);
                    }
                    break;
                }
            }
            writer.write("</" + fieldName + ">\n");
        }
        return offs;
    }


    private StorageImpl storage;
    private Writer      writer;
    private int[]       markedBitmap;
    private int[]       exportedBitmap;
    private int[]       compoundKeyTypes;
}
