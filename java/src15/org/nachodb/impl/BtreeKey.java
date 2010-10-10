package org.nachodb.impl;
import  org.nachodb.*;

class BtreeKey { 
    Key key;
    int oid;
    int oldOid;

    BtreeKey(Key key, int oid) { 
        this.key = key;
        this.oid = oid;
    }

    final void getStr(Page pg, int i) { 
        int len = BtreePage.getKeyStrSize(pg, i);
        int offs = BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, i);
        char[] sval = new char[len];
        for (int j = 0; j < len; j++) { 
            sval[j] = (char)Bytes.unpack2(pg.data, offs);
            offs += 2;
        }
        key = new Key(sval);
    }

    final void getByteArray(Page pg, int i) { 
        int len = BtreePage.getKeyStrSize(pg, i);
        int offs = BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, i);
        byte[] bval = new byte[len];
        System.arraycopy(pg.data, offs, bval, 0, len);
        key = new Key(bval);
    }


    final void extract(Page pg, int offs, int type) { 
        byte[] data = pg.data;

        switch (type) {
          case ClassDescriptor.tpBoolean:
            key = new Key(data[offs] != 0);
            break;
          case ClassDescriptor.tpByte:
            key = new Key(data[offs]);
            break;
          case ClassDescriptor.tpShort:
            key = new Key(Bytes.unpack2(data, offs));
            break;
          case ClassDescriptor.tpChar:
            key = new Key((char)Bytes.unpack2(data, offs));
            break;
          case ClassDescriptor.tpInt:
          case ClassDescriptor.tpObject:
          case ClassDescriptor.tpEnum:
            key = new Key(Bytes.unpack4(data, offs));
            break;
          case ClassDescriptor.tpLong:
          case ClassDescriptor.tpDate:
            key = new Key(Bytes.unpack8(data, offs));
            break;
          case ClassDescriptor.tpFloat:
            key = new Key(Float.intBitsToFloat(Bytes.unpack4(data, offs)));
            break;
          case ClassDescriptor.tpDouble:
            key = new Key(Double.longBitsToDouble(Bytes.unpack8(data, offs)));
            break;
          default:
            Assert.failed("Invalid type");
        }
    } 
    
    final void pack(Page pg, int i) { 
        byte[] dst = pg.data;
        switch (key.type) { 
          case ClassDescriptor.tpBoolean:
          case ClassDescriptor.tpByte:
            dst[BtreePage.firstKeyOffs + i] = (byte)key.ival;
            break;
          case ClassDescriptor.tpShort:
          case ClassDescriptor.tpChar:
            Bytes.pack2(dst, BtreePage.firstKeyOffs + i*2, (short)key.ival);
            break;
          case ClassDescriptor.tpInt:
          case ClassDescriptor.tpObject:
          case ClassDescriptor.tpEnum:
            Bytes.pack4(dst, BtreePage.firstKeyOffs + i*4, key.ival);
            break;
          case ClassDescriptor.tpLong:
          case ClassDescriptor.tpDate:
            Bytes.pack8(dst, BtreePage.firstKeyOffs + i*8,  key.lval);
            break;
          case ClassDescriptor.tpFloat:
            Bytes.pack4(dst, BtreePage.firstKeyOffs + i*4, Float.floatToIntBits((float)key.dval));
            break;
          case ClassDescriptor.tpDouble:
            Bytes.pack8(dst, BtreePage.firstKeyOffs + i*8, Double.doubleToLongBits(key.dval));
            break;
          default:
            Assert.failed("Invalid type");
        }
        Bytes.pack4(dst, BtreePage.firstKeyOffs + (BtreePage.maxItems - i - 1)*4, oid);
    }
}
