namespace Volante
{
    using System;
    using ClassDescriptor = Volante.Impl.ClassDescriptor;
    using FieldType = Volante.Impl.ClassDescriptor.FieldType;

    /// <summary> Class for specifying key value (neededd to access object by key usig index)
    /// </summary>
    public class Key
    {
        public readonly FieldType type;
        public readonly int ival;
        public readonly long lval;
        public readonly double dval;
        public readonly object oval;
        public readonly decimal dec;
        public readonly Guid guid;
        public readonly int inclusion;

        /// <summary> Constructor of boolean key (boundary is inclusive)
        /// </summary>
        public Key(bool v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of signed byte key (boundary is inclusive)
        /// </summary>
        public Key(sbyte v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of byte key (boundary is inclusive)
        /// </summary>
        public Key(byte v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of char key (boundary is inclusive)
        /// </summary>
        public Key(char v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of short key (boundary is inclusive)
        /// </summary>
        public Key(short v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of unsigned short key (boundary is inclusive)
        /// </summary>
        public Key(ushort v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of int key (boundary is inclusive)
        /// </summary>
        public Key(int v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of unsigned int key (boundary is inclusive)
        /// </summary>
        public Key(uint v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of enum key (boundary is inclusive)
        /// </summary>
        public Key(Enum v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of long key (boundary is inclusive)
        /// </summary>
        public Key(long v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of unsigned long key (boundary is inclusive)
        /// </summary>
        public Key(ulong v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of float key (boundary is inclusive)
        /// </summary>
        public Key(float v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of double key (boundary is inclusive)
        /// </summary>
        public Key(double v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of decimal key (boundary is inclusive)
        /// </summary>
        public Key(decimal v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of Guid key (boundary is inclusive)
        /// </summary>
        public Key(Guid v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of date key (boundary is inclusive)
        /// </summary>
        public Key(DateTime v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of string key (boundary is inclusive)
        /// </summary>
        public Key(string v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of key of user defined type (boundary is inclusive)
        /// </summary>
        public Key(IComparable v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of array of char key (boundary is inclusive)
        /// </summary>
        public Key(char[] v)
            : this(v, true)
        {
        }

        /// <summary> Constructor of array of byte key (boundary is inclusive)
        /// </summary>
        public Key(byte[] v)
            : this(v, true)
        {
        }

        /// <summary>
        /// Constructor of compound key (boundary is inclusive)
        /// </summary>
        /// <param name="v">array of compound key values</param>
        public Key(object[] v)
            : this(v, true)
        {
        }

        /// <summary>
        /// Constructor of compound key with two values (boundary is inclusive)
        /// </summary>
        /// <param name="v1">first value of compund key</param>
        /// <param name="v2">second value of compund key</param>
        public Key(object v1, object v2)
            : this(new object[] { v1, v2 }, true)
        {
        }

        /// <summary> Constructor of key with persistent object reference (boundary is inclusive)
        /// </summary>
        public Key(IPersistent v)
            : this(v, true)
        {
        }

        internal Key(FieldType type, bool inclusive)
        {
            this.type = type;
            this.inclusion = inclusive ? 1 : 0;
        }

        internal Key(FieldType type, int oid)
        {
            this.type = type;
            ival = oid;
            this.inclusion = 1;
        }

        /// <summary>Constructor of boolean key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(bool v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpBoolean, inclusive)
        {
            ival = v ? 1 : 0;
        }

        /// <summary>Constructor of signed byte key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(sbyte v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpSByte, inclusive)
        {
            ival = v;
        }

        /// <summary>Constructor of byte key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(byte v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpByte, inclusive)
        {
            ival = v;
        }

        /// <summary>Constructor of char key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(char v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpChar, inclusive)
        {
            ival = v;
        }

        /// <summary>Constructor of short key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(short v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpShort, inclusive)
        {
            ival = v;
        }

        /// <summary>Constructor of unsigned short key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(ushort v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpUShort, inclusive)
        {
            ival = v;
        }

        /// <summary>Constructor of int key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(Enum v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpEnum, inclusive)
        {
            ival = (int)(object)v;
        }

        /// <summary>Constructor of int key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(int v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpInt, inclusive)
        {
            ival = v;
        }

        /// <summary>Constructor of unsigned int key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(uint v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpUInt, inclusive)
        {
            ival = (int)v;
        }

        /// <summary>Constructor of long key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(long v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpLong, inclusive)
        {
            lval = v;
        }

        /// <summary>Constructor of unsigned long key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(ulong v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpULong, inclusive)
        {
            lval = (long)v;
        }

        /// <summary>Constructor of float key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(float v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpFloat, inclusive)
        {
            dval = v;
        }

        /// <summary>Constructor of double key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(double v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpDouble, inclusive)
        {
            dval = v;
        }

        /// <summary>Constructor of decimal key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(decimal v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpDecimal, inclusive)
        {
            dec = v;
        }

        /// <summary>Constructor of Guid key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(Guid v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpGuid, inclusive)
        {
            guid = v;
        }

        /// <summary>Constructor of date key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(DateTime v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpDate, inclusive)
        {
            lval = v.Ticks;
        }

        /// <summary>Constructor of string key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(string v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpString, inclusive)
        {
            oval = v;
        }

        /// <summary>Constructor of array of char key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(char[] v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpString, inclusive)
        {
            oval = v;
        }

        /// <summary>Constructor of array of byte key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive</param>
        public Key(byte[] v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpArrayOfByte, inclusive)
        {
            oval = v;
        }

        /// <summary>
        /// Constructor of compound key (boundary is inclusive)
        /// </summary>
        /// <param name="v">array of compound key values</param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive</param>
        public Key(object[] v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpArrayOfObject, inclusive)
        {
            oval = v;
        }

        /// <summary>
        /// Constructor of compound key with two values (boundary is inclusive)
        /// </summary>
        /// <param name="v1">first value of compund key</param>
        /// <param name="v2">second value of compund key</param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive</param>
        public Key(object v1, object v2, bool inclusive)
            : this(new object[] { v1, v2 }, inclusive)
        {
        }

        /// <summary>Constructor of key with persistent object reference
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(IPersistent v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpObject, inclusive)
        {
            ival = v != null ? v.Oid : 0;
            oval = v;
        }

        /// <summary>Constructor of key of user defined type
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// </param>
        public Key(IComparable v, bool inclusive)
            : this(ClassDescriptor.FieldType.tpRaw, inclusive)
        {
            oval = v;
        }
    }

    class KeyBuilder
    {
        public static Key getKeyFromObject(object o)
        {
            if (o == null)
            {
                return null;
            }
            else if (o is byte)
            {
                return new Key((byte)o);
            }
            else if (o is sbyte)
            {
                return new Key((sbyte)o);
            }
            else if (o is short)
            {
                return new Key((short)o);
            }
            else if (o is ushort)
            {
                return new Key((ushort)o);
            }
            else if (o is int)
            {
                return new Key((int)o);
            }
            else if (o is uint)
            {
                return new Key((uint)o);
            }
            else if (o is long)
            {
                return new Key((long)o);
            }
            else if (o is ulong)
            {
                return new Key((ulong)o);
            }
            else if (o is float)
            {
                return new Key((float)o);
            }
            else if (o is double)
            {
                return new Key((double)o);
            }
            else if (o is bool)
            {
                return new Key((bool)o);
            }
            else if (o is char)
            {
                return new Key((char)o);
            }
            else if (o is String)
            {
                return new Key((String)o);
            }
            else if (o is DateTime)
            {
                return new Key((DateTime)o);
            }
            else if (o is byte[])
            {
                return new Key((byte[])o);
            }
            else if (o is object[])
            {
                return new Key((object[])o);
            }
            else if (o is Enum)
            {
                return new Key((Enum)o);
            }
            else if (o is IPersistent)
            {
                return new Key((IPersistent)o);
            }
            else if (o is Guid)
            {
                return new Key((Guid)o);
            }
            else if (o is Decimal)
            {
                return new Key((Decimal)o);
            }
            else if (o is IComparable)
            {
                return new Key((IComparable)o);
            }
            throw new DatabaseException(DatabaseException.ErrorCode.UNSUPPORTED_TYPE);
        }
    }
}