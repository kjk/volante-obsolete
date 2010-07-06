namespace Perst
{
    using System;
    using ClassDescriptor = Perst.Impl.ClassDescriptor;
    using FieldType = Perst.Impl.ClassDescriptor.FieldType;
	
    /// <summary> Class for specifying key value (neededd to access obejct by key usig index)
    /// </summary>
    public class Key
    {
        public FieldType type;
        public int ival;
        public long lval;
        public double dval;
        public object oval;
        public int inclusion;
		
        /// <summary> Constructor of boolean key (boundary is inclusive)
        /// </summary>
        public Key(bool v):this(v, true)
        {
        }
		
        /// <summary> Constructor of signed byte key (boundary is inclusive)
        /// </summary>
        public Key(sbyte v):this(v, true)
        {
        }

        /// <summary> Constructor of byte key (boundary is inclusive)
        /// </summary>
        public Key(byte v):this(v, true)
        {
        }
		
        /// <summary> Constructor of char key (boundary is inclusive)
        /// </summary>
        public Key(char v):this(v, true)
        {
        }
		
        /// <summary> Constructor of short key (boundary is inclusive)
        /// </summary>
        public Key(short v):this(v, true)
        {
        }
		
        /// <summary> Constructor of unsigned short key (boundary is inclusive)
        /// </summary>
        public Key(ushort v):this(v, true)
        {
        }
		
        /// <summary> Constructor of int key (boundary is inclusive)
        /// </summary>
        public Key(int v):this(v, true)
        {
        }
		
        /// <summary> Constructor of unsigned int key (boundary is inclusive)
        /// </summary>
        public Key(uint v):this(v, true)
        {
        }
		
        /// <summary> Constructor of enum key (boundary is inclusive)
        /// </summary>
        public Key(Enum v):this(v, true)
        {
        }

        /// <summary> Constructor of long key (boundary is inclusive)
        /// </summary>
        public Key(long v):this(v, true)
        {
        }
		
        /// <summary> Constructor of unsigned long key (boundary is inclusive)
        /// </summary>
        public Key(ulong v):this(v, true)
        {
        }
		
        /// <summary> Constructor of float key (boundary is inclusive)
        /// </summary>
        public Key(float v):this(v, true)
        {
        }
		
        /// <summary> Constructor of double key (boundary is inclusive)
        /// </summary>
        public Key(double v):this(v, true)
        {
        }
		
        /// <summary> Constructor of date key (boundary is inclusive)
        /// </summary>
        public Key(System.DateTime v):this(v, true)
        {
        }
		
        /// <summary> Constructor of string key (boundary is inclusive)
        /// </summary>
        public Key(System.String v):this(v, true)
        {
        }
		
        /// <summary> Constructor of array of char key (boundary is inclusive)
        /// </summary>
        public Key(char[] v):this(v, true)
        {
        }
		
        /// <summary> Constructor of array of byte key (boundary is inclusive)
        /// </summary>
        public Key(byte[] v):this(v, true)
        {
        }
		
        /// <summary>
        /// Constructor of compound key (boundary is inclusive)
        /// </summary>
        /// <param name="v">array of compound key values</param>
        public Key(object[] v):this(v, true)
        {
        }    

        /// <summary>
        /// Constructor of compound key with two values (boundary is inclusive)
        /// </summary>
        /// <param name="v1">first value of compund key</param>
        /// <param name="v2">second value of compund key</param>
        public Key(object v1, object v2):this(new object[]{v1, v2}, true)
        {
        }    

        
        /// <summary> Constructor of key with persistent object reference (boundary is inclusive)
        /// </summary>
        public Key(IPersistent v):this(v, true)
        {
        }
		
        public Key(FieldType type, long lval, double dval, object oval, bool inclusive)
        {
            this.type = type;
            this.ival = (int) lval;
            this.lval = lval;
            this.dval = dval;
            this.oval = oval;
            this.inclusion = inclusive?1:0;
        }
		
        /// <summary> Constructor of boolean key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(bool v, bool inclusive):this(ClassDescriptor.FieldType.tpBoolean, v?1:0, 0.0, null, inclusive)
        {
        }
		
        /// <summary> Constructor of signed byte key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(sbyte v, bool inclusive):this(ClassDescriptor.FieldType.tpSByte, v, 0.0, null, inclusive)
        {
        }
		
        /// <summary> Constructor of byte key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(byte v, bool inclusive):this(ClassDescriptor.FieldType.tpByte, v, 0.0, null, inclusive)
        {
        }

        /// <summary> Constructor of char key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(char v, bool inclusive):this(ClassDescriptor.FieldType.tpChar, v, 0.0, null, inclusive)
        {
        }
		
        /// <summary> Constructor of short key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(short v, bool inclusive):this(ClassDescriptor.FieldType.tpShort, v, 0.0, null, inclusive)
        {
        }
		
        /// <summary> Constructor of unsigned short key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(ushort v, bool inclusive):this(ClassDescriptor.FieldType.tpUShort, v, 0.0, null, inclusive)
        {
        }
        
        /// <summary> Constructor of int key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(Enum v, bool inclusive):this(ClassDescriptor.FieldType.tpEnum, (int)(object)v, 0.0, null, inclusive)
        {
        }

        /// <summary> Constructor of int key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(int v, bool inclusive):this(ClassDescriptor.FieldType.tpInt, v, 0.0, null, inclusive)
        {
        }
		
        /// <summary> Constructor of unsigned int key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(uint v, bool inclusive):this(ClassDescriptor.FieldType.tpUInt, v, 0.0, null, inclusive)
        {
        }
		
        /// <summary> Constructor of long key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(long v, bool inclusive):this(ClassDescriptor.FieldType.tpLong, v, 0.0, null, inclusive)
        {
        }
		
        /// <summary> Constructor of unsigned long key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(ulong v, bool inclusive):this(ClassDescriptor.FieldType.tpULong, (long)v, 0.0, null, inclusive)
        {
        }
		
        /// <summary> Constructor of float key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(float v, bool inclusive):this(ClassDescriptor.FieldType.tpFloat, 0, v, null, inclusive)
        {
        }
		
        /// <summary> Constructor of double key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(double v, bool inclusive):this(ClassDescriptor.FieldType.tpDouble, 0, v, null, inclusive)
        {
        }
		
        /// <summary> Constructor of date key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(System.DateTime v, bool inclusive):this(ClassDescriptor.FieldType.tpDate, (v.Ticks - 621355968000000000) / 10000, 0.0, null, inclusive)
        {
        }

		static readonly char[] EMPTY_STRING = new char[0];

        /// <summary> Constructor of string key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(string v, bool inclusive):this(ClassDescriptor.FieldType.tpString, 0, 0.0, v == null ? EMPTY_STRING : v.ToCharArray(), inclusive)
        {
        }
		
        /// <summary> Constructor of array of char key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(char[] v, bool inclusive):this(ClassDescriptor.FieldType.tpString, 0, 0.0, v, inclusive)
        {
        }
		
        /// <summary> Constructor of array of byte key
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive</param>
        public Key(byte[] v, bool inclusive):this(ClassDescriptor.FieldType.tpArrayOfByte, 0, 0.0, v, inclusive)
        {
        }
		
        /// <summary>
        /// Constructor of compound key (boundary is inclusive)
        /// </summary>
        /// <param name="v">array of compound key values</param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive</param>
        public Key(object[] v, bool inclusive):this(ClassDescriptor.FieldType.tpArrayOfObject, 0, 0.0, v, inclusive) 
        { 
        }    

        /// <summary>
        /// Constructor of compound key with two values (boundary is inclusive)
        /// </summary>
        /// <param name="v1">first value of compund key</param>
        /// <param name="v2">second value of compund key</param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive</param>
        public Key(object v1, object v2, bool inclusive):this(new object[]{v1, v2}, inclusive)
        {
        }    

        /// <summary> Constructor of key with persistent object reference
        /// </summary>
        /// <param name="v">key value
        /// </param>
        /// <param name="inclusive">whether boundary is inclusive or exclusive
        /// 
        /// </param>
        public Key(IPersistent v, bool inclusive):this(ClassDescriptor.FieldType.tpObject, v.Oid, 0.0, null, inclusive)
        {
        }
    }
}