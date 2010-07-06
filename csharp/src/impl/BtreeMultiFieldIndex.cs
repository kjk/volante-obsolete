namespace Perst.Impl
{
    using System;
    using System.Collections;
    using System.Reflection;
    using Perst;
	
    class BtreeMultiFieldIndex:Btree, FieldIndex
    {
        internal String className;
        internal String[] fieldNames;
        internal ClassDescriptor.FieldType[] types;
        [NonSerialized()]
        Type cls;
        [NonSerialized()]
        FieldInfo[] fld;
 
        internal BtreeMultiFieldIndex()
        {
        }

        private void locateFields() 
        {
            fld = new FieldInfo[fieldNames.Length];
            for (int i = 0; i < fieldNames.Length; i++) 
            {
                fld[i] = cls.GetField(fieldNames[i], BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public); 
                if (fld[i] == null) 
                { 
                    throw new StorageError(StorageError.ErrorCode.INDEXED_FIELD_NOT_FOUND, className + "." + fieldNames[i]);
                }
            }
        }

        public override void OnLoad()
        {
            cls = ClassDescriptor.lookup(className);
            locateFields();
        }
		
        internal BtreeMultiFieldIndex(Type cls, string[] fieldNames, bool unique) 
        {
            this.cls = cls;
            this.unique = unique;
            this.fieldNames = fieldNames;
            this.className = cls.FullName;
            locateFields();
            type = ClassDescriptor.FieldType.tpArrayOfByte;        
            types = new ClassDescriptor.FieldType[fieldNames.Length];
            for (int i = 0; i < types.Length; i++) 
            {
                types[i] = checkType(fld[i].FieldType);
            }
        }

        protected override int compareByteArrays(byte[] key, byte[] item, int offs, int lengtn) 
        { 
            int o1 = 0;
            int o2 = offs;
            byte[] a1 = key;
            byte[] a2 = item;
            for (int i = 0; i < fld.Length; i++) 
            {
                int diff = 0;
                switch (types[i]) 
                { 
                    case ClassDescriptor.FieldType.tpBoolean:
                    case ClassDescriptor.FieldType.tpByte:
                        diff = a1[o1++] - a2[o2++];
                        break;
                    case ClassDescriptor.FieldType.tpSByte:
                        diff = (sbyte)a1[o1++] - (sbyte)a2[o2++];
                        break;
                    case ClassDescriptor.FieldType.tpShort:
                        diff = Bytes.unpack2(a1, o1) - Bytes.unpack2(a2, o2);
                        o1 += 2;
                        o2 += 2;
                        break;
                    case ClassDescriptor.FieldType.tpUShort:
                        diff = (ushort)Bytes.unpack2(a1, o1) - (ushort)Bytes.unpack2(a2, o2);
                        o1 += 2;
                        o2 += 2;
                        break;
                    case ClassDescriptor.FieldType.tpChar:
                        diff = (char)Bytes.unpack2(a1, o1) - (char)Bytes.unpack2(a2, o2);
                        o1 += 2;
                        o2 += 2;
                        break;
                    case ClassDescriptor.FieldType.tpInt:
                        diff = Bytes.unpack4(a1, o1) - Bytes.unpack4(a2, o2);
                        o1 += 4;
                        o2 += 4;
                        break;
                    case ClassDescriptor.FieldType.tpUInt:
                    case ClassDescriptor.FieldType.tpEnum:
                    case ClassDescriptor.FieldType.tpObject:
                    {
                        uint u1 = (uint)Bytes.unpack4(a1, o1);
                        uint u2 = (uint)Bytes.unpack4(a2, o2);
                        diff = u1 < u2 ? -1 : u1 == u2 ? 0 : 1;
                        o1 += 4;
                        o2 += 4;
                        break;
                    }
                    case ClassDescriptor.FieldType.tpLong:
                    {
                        long l1 = Bytes.unpack8(a1, o1);
                        long l2 = Bytes.unpack8(a2, o2);
                        diff = l1 < l2 ? -1 : l1 == l2 ? 0 : 1;
                        o1 += 8;
                        o2 += 8;
                        break;
                    }
                    case ClassDescriptor.FieldType.tpULong:
                    case ClassDescriptor.FieldType.tpDate:
                    {
                        ulong l1 = (ulong)Bytes.unpack8(a1, o1);
                        ulong l2 = (ulong)Bytes.unpack8(a2, o2);
                        diff = l1 < l2 ? -1 : l1 == l2 ? 0 : 1;
                        o1 += 8;
                        o2 += 8;
                        break;
                    }
                    
                    case ClassDescriptor.FieldType.tpFloat:
                    {
                        float f1 = BitConverter.ToSingle(BitConverter.GetBytes(Bytes.unpack4(a1, o1)), 0);
                        float f2 = BitConverter.ToSingle(BitConverter.GetBytes(Bytes.unpack4(a2, o2)), 0);
                        diff = f1 < f2 ? -1 : f1 == f2 ? 0 : 1;
                        o1 += 4;
                        o2 += 4;
                        break;
                    }
                    case ClassDescriptor.FieldType.tpDouble:
                    {
#if COMPACT_NET_FRAMEWORK 
                        double d1 = BitConverter.ToDouble(BitConverter.GetBytes(Bytes.unpack8(a1, o1)), 0);
                        double d2 = BitConverter.ToDouble(BitConverter.GetBytes(Bytes.unpack8(a2, o2)), 0);
#else
                        double d1 = BitConverter.Int64BitsToDouble(Bytes.unpack8(a1, o1));
                        double d2 = BitConverter.Int64BitsToDouble(Bytes.unpack8(a2, o2));
#endif
                        diff = d1 < d2 ? -1 : d1 == d2 ? 0 : 1;
                        o1 += 8;
                        o2 += 8;
                        break;
                    }

                    case ClassDescriptor.FieldType.tpString:
                    {
                        int len1 = Bytes.unpack4(a1, o1);
                        int len2 = Bytes.unpack4(a2, o2);
                        o1 += 4;
                        o2 += 4;
                        int len = len1 < len2 ? len1 : len2;
                        while (--len >= 0) 
                        { 
                            diff = (char)Bytes.unpack2(a1, o1) - (char)Bytes.unpack2(a2, o2);
                            if (diff != 0) 
                            { 
                                return diff;
                            }
                            o1 += 2;
                            o2 += 2;
                        }
                        diff = len1 - len2;
                        break;
                    }
                    case ClassDescriptor.FieldType.tpArrayOfByte:
                    {
                        int len1 = Bytes.unpack4(a1, o1);
                        int len2 = Bytes.unpack4(a2, o2);
                        o1 += 4;
                        o2 += 4;
                        int len = len1 < len2 ? len1 : len2;
                        while (--len >= 0) 
                        { 
                            diff = a1[o1++] - a2[o2++];
                            if (diff != 0) 
                            { 
                                return diff;
                            }
                        }
                        diff = len1 - len2;
                        break;
                    }
                    default:
                        Assert.Failed("Invalid type");
                        break;
                }
                if (diff != 0) 
                { 
                    return diff;
                }
            }
            return 0;
        }

        protected override object unpackByteArrayKey(Page pg, int pos) 
        {
            int offs = BtreePage.firstKeyOffs + BtreePage.getKeyStrOffs(pg, pos);
            byte[] data = pg.data;
            Object[] values = new Object[fld.Length];

            for (int i = 0; i < fld.Length; i++) 
            {
                Object v = null;
                switch (types[i]) 
                { 
                    case ClassDescriptor.FieldType.tpBoolean: 
                        v = data[offs++] != 0;
                        break;
				
                    case ClassDescriptor.FieldType.tpSByte: 
                        v = (sbyte)data[offs++];
                        break;
                   
                    case ClassDescriptor.FieldType.tpByte: 
                        v = data[offs++];
                        break;
 				
                    case ClassDescriptor.FieldType.tpShort: 
                        v = Bytes.unpack2(data, offs);
                        offs += 2;
                        break;

                    case ClassDescriptor.FieldType.tpUShort: 
                        v = (ushort)Bytes.unpack2(data, offs);
                        offs += 2;
                        break;
				
                    case ClassDescriptor.FieldType.tpChar: 
                        v = (char) Bytes.unpack2(data, offs);
                        offs += 2;
                        break;

                    case ClassDescriptor.FieldType.tpInt: 
                        v = Bytes.unpack4(data, offs);
                        offs += 4;
                        break;
                   
                    case ClassDescriptor.FieldType.tpEnum: 
                        v = Enum.ToObject(fld[i].FieldType, Bytes.unpack4(data, offs));
                        offs += 4;
                        break;
            
                    case ClassDescriptor.FieldType.tpUInt:
                        v = (uint)Bytes.unpack4(data, offs);
                        offs += 4;
                        break;
 
                    case ClassDescriptor.FieldType.tpObject: 
                    {
                        int oid = Bytes.unpack4(data, offs);
                        v = oid == 0 ? null : ((StorageImpl)Storage).lookupObject(oid, null);
                        offs += 4;
                        break;
                    }
                    case ClassDescriptor.FieldType.tpLong: 
                        v = Bytes.unpack8(data, offs);
                        offs += 8;
                        break;

                    case ClassDescriptor.FieldType.tpDate: 
                    {
                        v = new DateTime(Bytes.unpack8(data, offs));
                        offs += 8;
                        break;
                    }
                    case ClassDescriptor.FieldType.tpULong: 
                        v = (ulong)Bytes.unpack8(data, offs);
                        offs += 8;
                        break;
 				
                    case ClassDescriptor.FieldType.tpFloat: 
                        v = BitConverter.ToSingle(BitConverter.GetBytes(Bytes.unpack4(data, offs)), 0);
                        offs += 4;
                        break;

                    case ClassDescriptor.FieldType.tpDouble: 
#if COMPACT_NET_FRAMEWORK 
                        v = BitConverter.ToDouble(BitConverter.GetBytes(Bytes.unpack8(data, offs)), 0);
#else
                        v = BitConverter.Int64BitsToDouble(Bytes.unpack8(data, offs));
#endif
                        offs += 8;
                        break;

                    case ClassDescriptor.FieldType.tpString:
                    {
                        int len = Bytes.unpack4(data, offs);
                        offs += 4;
                        char[] sval = new char[len];
                        for (int j = 0; j < len; j++)
                        {
                            sval[j] = (char) Bytes.unpack2(pg.data, offs);
                            offs += 2;
                        }
                        v = new String(sval);
                        break;
                    }
                    case ClassDescriptor.FieldType.tpArrayOfByte:
                    {
                        int len = Bytes.unpack4(data, offs);
                        offs += 4;
                        byte[] val = new byte[len];
                        Array.Copy(pg.data, offs, val, 0, len);
                        offs += len;
                        v = val;
                        break;
                    }
                    default: 
                        Assert.Failed("Invalid type");
                        break;
                }
                values[i] = v;
            }
            return values;
        }


        private Key extractKey(IPersistent obj) 
        { 
            ByteBuffer buf = new ByteBuffer();
            int dst = 0;
            for (int i = 0; i < fld.Length; i++) 
            { 
                dst = packKeyPart(buf, dst, types[i], fld[i].GetValue(obj));
            }
            return new Key(buf.toArray());
        }

        private Key convertKey(Key key) 
        { 
            if (key == null) 
            { 
                return null;
            }
            if (key.type != ClassDescriptor.FieldType.tpArrayOfObject) 
            { 
                throw new StorageError(StorageError.ErrorCode.INCOMPATIBLE_KEY_TYPE);
            }
            Object[] values = (Object[])key.oval;
            ByteBuffer buf = new ByteBuffer();
            int dst = 0;
                
            for (int i = 0; i < fld.Length; i++) 
            { 
                dst = packKeyPart(buf, dst, types[i], values[i]);
            }
            return new Key(buf.toArray(), key.inclusion != 0);
        }

        private int packKeyPart(ByteBuffer buf, int dst, ClassDescriptor.FieldType type, object val)
        {
            switch (type) 
            {
                case ClassDescriptor.FieldType.tpBoolean:
                    buf.extend(dst+1);
                    buf.arr[dst++] = (byte)((bool)val ? 1 : 0);
                    break;
                case ClassDescriptor.FieldType.tpByte:
                    buf.extend(dst+1);
                    buf.arr[dst++] = (byte)val;
                    break;
                case ClassDescriptor.FieldType.tpSByte:
                    buf.extend(dst+1);
                    buf.arr[dst++] = (byte)(sbyte)val;
                    break;
                case ClassDescriptor.FieldType.tpShort:
                    buf.extend(dst+2);
                    Bytes.pack2(buf.arr, dst, (short)val);
                    dst += 2;
                    break;
                case ClassDescriptor.FieldType.tpUShort:
                    buf.extend(dst+2);
                    Bytes.pack2(buf.arr, dst, (short)(ushort)val);
                    dst += 2;
                    break;
                case ClassDescriptor.FieldType.tpChar:
                    buf.extend(dst+2);
                    Bytes.pack2(buf.arr, dst, (short)(char)val);
                    dst += 2;
                    break;
                case ClassDescriptor.FieldType.tpInt:
                    buf.extend(dst+4);
                    Bytes.pack4(buf.arr, dst, (int)val);
                    dst += 4;
                    break;            
                case ClassDescriptor.FieldType.tpUInt:
                    buf.extend(dst+4);
                    Bytes.pack4(buf.arr, dst, (int)(uint)val);
                    dst += 4;
                    break;            
                case ClassDescriptor.FieldType.tpObject:
                    buf.extend(dst+4);
                    Bytes.pack4(buf.arr, dst, val != null ? (int)((IPersistent)val).Oid : 0);
                    dst += 4;
                    break;
                case ClassDescriptor.FieldType.tpLong:
                    buf.extend(dst+8);
                    Bytes.pack8(buf.arr, dst, (long)val);
                    dst += 8;
                    break;            
                case ClassDescriptor.FieldType.tpULong:
                    buf.extend(dst+8);
                    Bytes.pack8(buf.arr, dst, (long)(ulong)val);
                    dst += 8;
                    break;            
                case ClassDescriptor.FieldType.tpDate:
                {
                    DateTime d = (DateTime)val;
                    buf.extend(dst+8);
                    Bytes.pack8(buf.arr, dst, d.Ticks);
                    dst += 8;
                    break;            
                }                   
                case ClassDescriptor.FieldType.tpFloat: 
                    buf.extend(dst+4);
                    Bytes.pack4(buf.arr, dst, BitConverter.ToInt32(BitConverter.GetBytes((float)val), 0));
                    dst += 4;
                    break;
				
                case ClassDescriptor.FieldType.tpDouble: 
                    buf.extend(dst+8);
#if COMPACT_NET_FRAMEWORK 
                    Bytes.pack8(buf.arr, dst, BitConverter.ToInt64(BitConverter.GetBytes((double)val), 0));
#else
                    Bytes.pack8(buf.arr, dst, BitConverter.DoubleToInt64Bits((double)val));
#endif
                    dst += 8;
                    break;
	
                case ClassDescriptor.FieldType.tpString:
                {
                    buf.extend(dst+4);
                    string str = (string)val;
                    if (str != null) 
                    { 
                        int len = str.Length;
                        Bytes.pack4(buf.arr, dst, len);
                        dst += 4;
                        buf.extend(dst + len*2);
                        for (int j = 0; j < len; j++) 
                        { 
                            Bytes.pack2(buf.arr, dst, (short)str[j]);
                            dst += 2;
                        }
                    } 
                    else 
                    { 
                        Bytes.pack4(buf.arr, dst, 0);
                        dst += 4;
                    }
                    break;
                }
                case ClassDescriptor.FieldType.tpArrayOfByte:
                {
                    buf.extend(dst+4);
                    byte[] arr = (byte[])val;
                    if (arr != null) 
                    { 
                        int len = arr.Length;
                        Bytes.pack4(buf.arr, dst, len);
                        dst += 4;                          
                        buf.extend(dst + len);
                        Array.Copy(arr, 0, buf.arr, dst, len);
                        dst += len;
                    } 
                    else 
                    { 
                        Bytes.pack4(buf.arr, dst, 0);
                        dst += 4;
                    }
                    break;
                }
                case ClassDescriptor.FieldType.tpEnum:
                    buf.extend(dst+4);
                    Bytes.pack4(buf.arr, dst, (int)val);
                    dst += 4;
                    break;

                default:
                    Assert.Failed("Invalid type");
                    break;
            }
            return dst;
        }
 
        public bool Put(IPersistent obj) 
        {
            return base.insert(extractKey(obj), obj, false);
        }

        public void Set(IPersistent obj) 
        {
            base.insert(extractKey(obj), obj, true);
        }

        public void Remove(IPersistent obj) 
        {
            base.remove(new BtreeKey(extractKey(obj), obj.Oid));
        }
        
        public void Remove(Key key) 
        {
            base.Remove(convertKey(key));
        }       

        public bool Contains(IPersistent obj) 
        {
            Key key = extractKey(obj);
            if (unique) 
            { 
                return base.Get(key) != null;
            } 
            else 
            { 
                IPersistent[] mbrs = Get(key, key);
                for (int i = 0; i < mbrs.Length; i++) 
                { 
                    if (mbrs[i] == obj) 
                    { 
                        return true;
                    }
                }
                return false;
            }
        }

        public void Append(IPersistent obj) 
        {
            throw new StorageError(StorageError.ErrorCode.UNSUPPORTED_INDEX_TYPE);
        }

        public override IPersistent[] Get(Key from, Key till)
        {
            ArrayList list = new ArrayList();
            if (root != 0)
            {
                BtreePage.find((StorageImpl) Storage, root, convertKey(from), convertKey(till), this, height, list);
            }
            return (IPersistent[]) list.ToArray(cls);
        }

        public override IPersistent[] ToArray() 
        {
            IPersistent[] arr = (IPersistent[])Array.CreateInstance(cls, nElems);
            if (root != 0) 
            { 
                BtreePage.traverseForward((StorageImpl)Storage, root, type, height, arr, 0);
            }
            return arr;
        }

        public override IPersistent Get(Key key) 
        {
            return base.Get(convertKey(key));
        }
 
        public override IEnumerable Range(Key from, Key till, IterationOrder order) 
        { 
            return base.Range(convertKey(from), convertKey(till), order);
        }


        public override IDictionaryEnumerator GetDictionaryEnumerator(Key from, Key till, IterationOrder order) 
        {
            return base.GetDictionaryEnumerator(convertKey(from), convertKey(till), order);
        }
     }
}