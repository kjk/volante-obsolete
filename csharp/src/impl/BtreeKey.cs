namespace Perst.Impl
{
    using System;
    using Perst;
	
    class BtreeKey
    {
        internal Key key;
        internal int oid;
		
        internal BtreeKey(Key key, int oid)
        {
            this.key = key;
            this.oid = oid;
        }
		
        internal void  getStr(Page pg, int i)
        {
            int len = BtreePage.getKeyStrSize(pg, i);
            int offs = BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, i);
            char[] sval = new char[len];
            for (int j = 0; j < len; j++)
            {
                sval[j] = (char) Bytes.unpack2(pg.data, offs);
                offs += 2;
            }
            key = new Key(sval);
        }
		
		
        internal void  extract(Page pg, int offs, ClassDescriptor.FieldType type)
        {
            int i, len;
            char[] chars;
            byte[] data = pg.data;
			
            switch (type)
            {
                case ClassDescriptor.FieldType.tpBoolean: 
                    key = new Key(data[offs] != 0);
                    break;
				
                case ClassDescriptor.FieldType.tpSByte: 
                    key = new Key((sbyte)data[offs]);
                    break;
                case ClassDescriptor.FieldType.tpByte: 
                    key = new Key(data[offs]);
                    break;
				
                case ClassDescriptor.FieldType.tpShort: 
                    key = new Key(Bytes.unpack2(data, offs));
                    break;
                case ClassDescriptor.FieldType.tpUShort: 
                    key = new Key((ushort)Bytes.unpack2(data, offs));
                    break;
				
				
                case ClassDescriptor.FieldType.tpChar: 
                    key = new Key((char) Bytes.unpack2(data, offs));
                    break;
				
                case ClassDescriptor.FieldType.tpInt: 
                    key = new Key(Bytes.unpack4(data, offs));
                    break;
                case ClassDescriptor.FieldType.tpEnum: 
                case ClassDescriptor.FieldType.tpUInt: 
                case ClassDescriptor.FieldType.tpObject: 
                    key = new Key((uint)Bytes.unpack4(data, offs));
                    break;
				
                case ClassDescriptor.FieldType.tpLong: 
                    key = new Key(Bytes.unpack8(data, offs));
                    break;
                case ClassDescriptor.FieldType.tpDate: 
                case ClassDescriptor.FieldType.tpULong: 
                    key = new Key((ulong)Bytes.unpack8(data, offs));
                    break;
				
                case ClassDescriptor.FieldType.tpFloat: 
                    key = new Key(BitConverter.ToSingle(BitConverter.GetBytes(Bytes.unpack4(data, offs)), 0));
                    break;
				
                case ClassDescriptor.FieldType.tpDouble: 
                    key = new Key(BitConverter.Int64BitsToDouble(Bytes.unpack8(data, offs)));
                    break;
				
                default: 
                    Assert.failed("Invalid type");
                    break;
				
            }
        }
		
        internal void  pack(Page pg, int i)
        {
            byte[] dst = pg.data;
            switch (key.type)
            {
                case ClassDescriptor.FieldType.tpBoolean: 
                case ClassDescriptor.FieldType.tpSByte: 
                case ClassDescriptor.FieldType.tpByte: 
                    dst[BtreePage.firstKeyOffs + i] = (byte) key.ival;
                    break;
				
                case ClassDescriptor.FieldType.tpShort: 
                case ClassDescriptor.FieldType.tpUShort: 
                case ClassDescriptor.FieldType.tpChar: 
                    Bytes.pack2(dst, BtreePage.firstKeyOffs + i * 2, (short) key.ival);
                    break;
				
                case ClassDescriptor.FieldType.tpInt: 
                case ClassDescriptor.FieldType.tpUInt: 
                case ClassDescriptor.FieldType.tpEnum: 
                case ClassDescriptor.FieldType.tpObject: 
                    Bytes.pack4(dst, BtreePage.firstKeyOffs + i * 4, key.ival);
                    goto case ClassDescriptor.FieldType.tpLong;
				
                case ClassDescriptor.FieldType.tpLong: 
                case ClassDescriptor.FieldType.tpULong: 
                case ClassDescriptor.FieldType.tpDate: 
                    Bytes.pack8(dst, BtreePage.firstKeyOffs + i * 8, key.lval);
                    break;
				
                case ClassDescriptor.FieldType.tpFloat: 
                    Bytes.pack4(dst, BtreePage.firstKeyOffs + i * 4, BitConverter.ToInt32(BitConverter.GetBytes((float)key.dval), 0));
                    break;
				
                case ClassDescriptor.FieldType.tpDouble: 
                    Bytes.pack8(dst, BtreePage.firstKeyOffs + i * 8, BitConverter.DoubleToInt64Bits(key.dval));
                    break;
				
                default: 
                    Assert.failed("Invalid type");
                    break;
				
            }
            Bytes.pack4(dst, BtreePage.firstKeyOffs + (BtreePage.maxItems - i - 1) * 4, oid);
        }
    }
}