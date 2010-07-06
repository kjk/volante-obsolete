package org.garret.perst.impl;

import java.io.*;
import java.util.Date;
import java.lang.reflect.Field;

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
                            if (typeOid == storage.btreeClassOid) { 
                                exportIndex(oid, obj);
                            } else if (typeOid == storage.btree2ClassOid) { 
                                exportFieldIndex(oid, obj);
                            } else { 
                                ClassDescriptor desc = (ClassDescriptor)storage.lookupObject(typeOid, ClassDescriptor.class);
                                writer.write(" <" + desc.name + " id=\"" + oid + "\">\n");
                                exportObject(desc, obj, ObjectHeader.sizeof, 2);
                                writer.write(" </" + desc.name + ">\n");
                            }
                            nExportedObjects += 1;
                        }
                    }
                }
            }                            
        } while (nExportedObjects != 0);
        writer.write("</database>\n");        
    }

    final void exportIndex(int oid,  byte[] data) throws IOException 
    { 
        Btree btree = new Btree(data, ObjectHeader.sizeof);
        storage.assignOid(btree, oid);
        writer.write(" <btree-index id=\"" + oid + "\" unique=\"" + (btree.unique ? '1' : '0') 
                     + "\" type=\"" + ClassDescriptor.signature[btree.type] + "\">\n");
        btree.export(this);
        writer.write(" </btree-index>\n");
    }

    final void exportFieldIndex(int oid,  byte[] data) throws IOException
    { 
        Btree btree = new Btree(data, ObjectHeader.sizeof);
        storage.assignOid(btree, oid);
        writer.write(" <btree-index id=\"" + oid + "\" unique=\"" + (btree.unique ? '1' : '0') 
                     + "\" class=");
        int offs = exportString(data, Btree.sizeof);
        writer.write(" field=");
        exportString(data, offs);
        writer.write(">\n");
        btree.export(this);
        writer.write(" </btree-index>\n");
    }

    final void exportAssoc(int oid, byte[] body, int offs, int size, int type) throws IOException
    {
        writer.write("  <ref id=\"" + oid + "\" key=\"");
        if ((exportedBitmap[oid >> 5] & (1 << (oid & 31))) == 0) { 
            markedBitmap[oid >> 5] |= 1 << (oid & 31);
        }
        switch (type) { 
            case ClassDescriptor.tpBoolean:
                writer.write(body[offs] != 0 ? "1" : "0");
                break;
            case ClassDescriptor.tpByte:
                writer.write(Integer.toString(body[offs]));
                break;
            case ClassDescriptor.tpChar:
                writer.write(Integer.toString((char)Bytes.unpack2(body, offs)));
                break;
            case ClassDescriptor.tpShort:
                writer.write(Integer.toString(Bytes.unpack2(body, offs)));
                break;
            case ClassDescriptor.tpInt:
            case ClassDescriptor.tpObject:
                writer.write(Integer.toString(Bytes.unpack4(body, offs)));
                break;
            case ClassDescriptor.tpLong:
                writer.write(Long.toString(Bytes.unpack8(body, offs)));
                break;
            case ClassDescriptor.tpFloat:
                writer.write(Float.toString(Float.intBitsToFloat(Bytes.unpack4(body, offs))));
                break;
            case ClassDescriptor.tpDouble:
                writer.write(Double.toString(Double.longBitsToDouble(Bytes.unpack8(body, offs))));
                break;
            case ClassDescriptor.tpString:
                for (int i = 0; i < size; i++) { 
                    exportChar((char)Bytes.unpack2(body, offs));
                    offs += 2;
                }
                break;
            case ClassDescriptor.tpDate:
            {
                long msec = Bytes.unpack8(body, offs);
                if (msec >= 0) { 
                    writer.write(XMLImporter.httpFormatter.format(new Date(msec)));
                } else { 
                    writer.write("null");
                }
                break;
            }
        }
        writer.write("\"/>\n");
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
            writer.write("null");
        } else {
            writer.write('\"');
            while (--len >= 0) {
                byte b = body[offs++];
                writer.write(hexDigit[b >> 4]);
                writer.write(hexDigit[b & 0xF]);
            }
            writer.write('\"');
        }
        return offs;
    }

    final int exportObject(ClassDescriptor desc, byte[] body, int offs, int indent) throws IOException {
        Field[] all = desc.allFields;
        int[] type = desc.fieldTypes;

        for (int i = 0, n = all.length; i < n; i++) { 
            Field f = all[i];
            indentation(indent);
            writer.write("<" + f.getName() + ">");
            switch (type[i]) { 
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
                {
                    int oid = Bytes.unpack4(body, offs);
                    writer.write("<ref id=\"" + oid + "\"/>");
                    if (oid != 0 && (exportedBitmap[oid >> 5] & (1 << (oid & 31))) == 0) { 
                        markedBitmap[oid >> 5] |= 1 << (oid & 31);
                    }
                    offs += 4;
                    break;
                }
                case ClassDescriptor.tpValue:
                    writer.write('\n');
                    offs = exportObject(storage.getClassDescriptor(f.getType()), body, offs, indent+1);
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
                            writer.write("<array-element>" + (body[offs++] != 0 ? "1" : "0") + "</array-element>\n");
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
                            writer.write("<array-element>" + (Bytes.unpack2(body, offs) & 0xFFFF) + "</array-element>\n");
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
                            writer.write("<array-element>" + Bytes.unpack2(body, offs) + "</array-element>\n");
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
                        while (--len >= 0) { 
                            indentation(indent+1);
                            writer.write("<array-element>" + Bytes.unpack4(body, offs) + "</array-element>\n");
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
                            writer.write("<array-element>" + Bytes.unpack8(body, offs) + "</array-element>\n");
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
                            writer.write("<array-element>" 
                                         + Float.intBitsToFloat(Bytes.unpack4(body, offs)) 
                                         + "</array-element>\n");
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
                            writer.write("<array-element>" 
                                         + Double.longBitsToDouble(Bytes.unpack8(body, offs)) 
                                         + "</array-element>\n");
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
                                writer.write("<array-element>\"");
                                writer.write(XMLImporter.httpFormatter.format(new Date(msec)));
                                writer.write("\"</array-element>\n");
                            } else { 
                                writer.write("<array-element>null</array-element>\n");
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
                            writer.write("<array-element>");
                            offs = exportString(body, offs);
                            writer.write("</array-element>\n");
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
                            writer.write("<array-element><ref id=\"" + oid + "\"/></array-element>\n");
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
                            writer.write("<array-element>\n");
                            offs = exportObject(storage.getClassDescriptor(f.getType()), body, offs, indent+2);
                            indentation(indent+1);
                            writer.write("</array-element>\n");
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
                            writer.write("<array-element>");
                            offs = exportBinary(body, offs);
                            writer.write("</array-element>\n");
                        }
                        indentation(indent);
                    }
                    break;
                }
            }
            writer.write("</" + f.getName() + ">\n");
        }
        return offs;
    }


    private StorageImpl storage;
    private Writer      writer;
    private int[]       markedBitmap;
    private int[]       exportedBitmap;
}
