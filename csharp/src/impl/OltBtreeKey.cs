#if WITH_OLD_BTREE
namespace Volante.Impl
{
    using System;
    using Volante;
    using System.Diagnostics;

    class OldBtreeKey
    {
        internal Key key;
        internal int oid;
        internal int oldOid;

        internal OldBtreeKey(Key key, int oid)
        {
            this.key = key;
            this.oid = oid;
        }

        internal void getStr(Page pg, int i)
        {
            int len = OldBtreePage.getKeyStrSize(pg, i);
            int offs = OldBtreePage.firstKeyOffs + OldBtreePage.getKeyStrOffs(pg, i);
            char[] sval = new char[len];
            for (int j = 0; j < len; j++)
            {
                sval[j] = (char)Bytes.unpack2(pg.data, offs);
                offs += 2;
            }
            key = new Key(sval);
        }

        internal void getByteArray(Page pg, int i)
        {
            int len = OldBtreePage.getKeyStrSize(pg, i);
            int offs = OldBtreePage.firstKeyOffs + OldBtreePage.getKeyStrOffs(pg, i);
            byte[] bval = new byte[len];
            Array.Copy(pg.data, offs, bval, 0, len);
            key = new Key(bval);
        }

        internal void extract(Page pg, int offs, ClassDescriptor.FieldType type)
        {
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
                    key = new Key((char)Bytes.unpack2(data, offs));
                    break;

                case ClassDescriptor.FieldType.tpInt:
                    key = new Key(Bytes.unpack4(data, offs));
                    break;
                case ClassDescriptor.FieldType.tpEnum:
                case ClassDescriptor.FieldType.tpUInt:
                case ClassDescriptor.FieldType.tpObject:
                case ClassDescriptor.FieldType.tpOid:
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
                    key = new Key(Bytes.unpackF4(data, offs));
                    break;

                case ClassDescriptor.FieldType.tpDouble:
                    key = new Key(Bytes.unpackF8(data, offs));
                    break;

                case ClassDescriptor.FieldType.tpGuid:
                    key = new Key(Bytes.unpackGuid(data, offs));
                    break;

                case ClassDescriptor.FieldType.tpDecimal:
                    key = new Key(Bytes.unpackDecimal(data, offs));
                    break;

                default:
                    Debug.Assert(false, "Invalid type");
                    break;

            }
        }

        internal void pack(Page pg, int i)
        {
            byte[] dst = pg.data;
            switch (key.type)
            {
                case ClassDescriptor.FieldType.tpBoolean:
                case ClassDescriptor.FieldType.tpSByte:
                case ClassDescriptor.FieldType.tpByte:
                    dst[OldBtreePage.firstKeyOffs + i] = (byte)key.ival;
                    break;

                case ClassDescriptor.FieldType.tpShort:
                case ClassDescriptor.FieldType.tpUShort:
                case ClassDescriptor.FieldType.tpChar:
                    Bytes.pack2(dst, OldBtreePage.firstKeyOffs + i * 2, (short)key.ival);
                    break;

                case ClassDescriptor.FieldType.tpInt:
                case ClassDescriptor.FieldType.tpUInt:
                case ClassDescriptor.FieldType.tpEnum:
                case ClassDescriptor.FieldType.tpObject:
                case ClassDescriptor.FieldType.tpOid:
                    Bytes.pack4(dst, OldBtreePage.firstKeyOffs + i * 4, key.ival);
                    break;

                case ClassDescriptor.FieldType.tpLong:
                case ClassDescriptor.FieldType.tpULong:
                case ClassDescriptor.FieldType.tpDate:
                    Bytes.pack8(dst, OldBtreePage.firstKeyOffs + i * 8, key.lval);
                    break;

                case ClassDescriptor.FieldType.tpFloat:
                    Bytes.packF4(dst, OldBtreePage.firstKeyOffs + i * 4, (float)key.dval);
                    break;

                case ClassDescriptor.FieldType.tpDouble:
                    Bytes.packF8(dst, OldBtreePage.firstKeyOffs + i * 8, key.dval);
                    break;

                case ClassDescriptor.FieldType.tpDecimal:
                    Bytes.packDecimal(dst, OldBtreePage.firstKeyOffs + i * 16, key.dec);
                    break;

                case ClassDescriptor.FieldType.tpGuid:
                    Bytes.packGuid(dst, OldBtreePage.firstKeyOffs + i * 16, key.guid);
                    break;


                default:
                    Debug.Assert(false, "Invalid type");
                    break;

            }
            Bytes.pack4(dst, OldBtreePage.firstKeyOffs + (OldBtreePage.maxItems - i - 1) * 4, oid);
        }
    }
}
#endif