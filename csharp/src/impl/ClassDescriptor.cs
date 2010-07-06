namespace Perst.Impl
{
    using System;
    using System.Collections;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using Perst;
	
    public sealed class ClassDescriptor:Persistent
    {
        internal ClassDescriptor next;
        internal System.String name;
        internal int nFields;
		
        [NonSerialized()]
        internal System.Reflection.FieldInfo[] allFields;
        [NonSerialized()]
        internal FieldType[] fieldTypes;
        [NonSerialized()]
        internal System.Type cls;
        [NonSerialized()]
        internal bool hasSubclasses;
        [NonSerialized()]
        internal bool hasReferences;
        [NonSerialized()]
        internal System.Reflection.ConstructorInfo defaultConstructor;
		
        public enum FieldType 
        {
            tpBoolean,
            tpByte,
            tpSByte,
            tpShort, 
            tpUShort,
            tpChar,
            tpEnum,
            tpInt,
            tpUInt,
            tpLong,
            tpULong,
            tpFloat,
            tpDouble,
            tpString,
            tpDate,
            tpObject,
            tpValue,
            tpRaw,
            tpLink,
            tpArrayOfBoolean,
            tpArrayOfByte,
            tpArrayOfSByte,
            tpArrayOfShort, 
            tpArrayOfUShort,
            tpArrayOfChar,
            tpArrayOfEnum,
            tpArrayOfInt,
            tpArrayOfUInt,
            tpArrayOfLong,
            tpArrayOfULong,
            tpArrayOfFloat,
            tpArrayOfDouble,
            tpArrayOfString,
            tpArrayOfDate,
            tpArrayOfObject,
            tpArrayOfValue,
            tpArrayOfRaw
        };
		
        internal static int[] Sizeof = new int[] {1, 1, 1, 2, 2, 2, 4, 4, 4, 8, 8, 4, 8, 0, 8, 4};
		
        internal static System.Type[] defaultConstructorProfile = new System.Type[0];
        internal static System.Object[] noArgs = new System.Object[0];
		
        internal Object newInstance()
        {
            try
            {
                return defaultConstructor.Invoke(noArgs);
            }
            catch (System.Exception x)
            {
                throw new StorageError(StorageError.ErrorCode.CONSTRUCTOR_FAILURE, cls, x);
            }
        }
		
        internal void  buildFieldList(System.Type cls, ArrayList list)
        {
            System.Type superclass = cls.BaseType;
            if (superclass != null)
            {
                buildFieldList(superclass, list);
            }
            System.Reflection.FieldInfo[] flds = cls.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.DeclaredOnly);
            for (int i = 0; i < flds.Length; i++)
            {
                System.Reflection.FieldInfo f = flds[i];
                if (!f.IsNotSerialized && !f.IsStatic)
                {
                    list.Add(f);
                }
            }
        }
		
        public static FieldType getTypeCode(System.Type c)
        {
            FieldType type;
            if (c.Equals(typeof(byte)))
            {
                type = FieldType.tpByte;
            }
            else if (c.Equals(typeof(sbyte)))
            {
                type = FieldType.tpSByte;
            }
            else if (c.Equals(typeof(short)))
            {
                type = FieldType.tpShort;
            }
            else if (c.Equals(typeof(ushort)))
            {
                type = FieldType.tpUShort;
            }
            else if (c.Equals(typeof(char)))
            {
                type = FieldType.tpChar;
            }
            else if (c.Equals(typeof(int)))
            {
                type = FieldType.tpInt;
            }
            else if (c.Equals(typeof(uint)))
            {
                type = FieldType.tpUInt;
            }
            else if (c.Equals(typeof(long)))
            {
                type = FieldType.tpLong;
            }
            else if (c.Equals(typeof(ulong)))
            {
                type = FieldType.tpULong;
            }
            else if (c.Equals(typeof(float)))
            {
                type = FieldType.tpFloat;
            }
            else if (c.Equals(typeof(double)))
            {
                type = FieldType.tpDouble;
            }
            else if (c.Equals(typeof(System.String)))
            {
                type = FieldType.tpString;
            }
            else if (c.Equals(typeof(bool)))
            {
                type = FieldType.tpBoolean;
            }
            else if (c.Equals(typeof(System.DateTime)))
            {
                type = FieldType.tpDate;
            }
            else if (c.IsEnum) 
            { 
                type = FieldType.tpEnum;
            }
            else if (typeof(IPersistent).IsAssignableFrom(c))
            {
                type = FieldType.tpObject;
            }
            else if (typeof(ValueType).IsAssignableFrom(c))
            {
                type = FieldType.tpValue;
            }
            else if (c.Equals(typeof(Link)))
            {
                type = FieldType.tpLink;
            }
            else if (c.IsArray)
            {
                type = getTypeCode(c.GetElementType());
                if ((int)type >= (int)FieldType.tpLink)
                {
                    throw new StorageError(StorageError.ErrorCode.UNSUPPORTED_TYPE, c);
                }
                type = (FieldType)((int)type + (int)FieldType.tpArrayOfBoolean);
            }
            else
            {
                type = FieldType.tpRaw;
            }
            return type;
        }
		
        internal ClassDescriptor()
        {
        }
		
        internal ClassDescriptor(System.Type cls)
        {
            this.cls = cls;
            name = cls.FullName;
            build();
            nFields = allFields.Length;
        }
		
        internal static bool FindTypeByName(Type t, object name) 
        { 
            return t.FullName.Equals(name);
        }

        internal static Type lookup(String name)
        {
            Type cls = null;
            foreach (Assembly ass in AppDomain.CurrentDomain.GetAssemblies()) 
            { 
                foreach (Module mod in ass.GetModules()) 
                { 
                    foreach (Type t in mod.FindTypes(new TypeFilter(FindTypeByName), name)) 
                    { 
                        if (cls != null) 
                        { 
                            throw new StorageError(StorageError.ErrorCode.AMBIGUITY_CLASS, name);
                        } 
                        else 
                        { 
                            cls = t;
                        }
                    }
                }
            }
            if (cls == null) 
            {
                throw new StorageError(StorageError.ErrorCode.CLASS_NOT_FOUND, name);
            }
            return cls;
        }

        public void  resolve()
        {
            if (cls == null) 
            {
                cls = lookup(name);
                build();
                if (nFields != allFields.Length)
                {
                    throw new StorageError(StorageError.ErrorCode.SCHEMA_CHANGED, cls);
                }
            }
        }
    
		
        internal void  build()
        {
            ArrayList list = new ArrayList();
            buildFieldList(cls, list);
            int nFields = list.Count;
            allFields = (System.Reflection.FieldInfo[]) list.ToArray(typeof(System.Reflection.FieldInfo));
            fieldTypes = new FieldType[nFields];
            for (int i = 0; i < nFields; i++)
            {
                Type fieldType = allFields[i].FieldType;
                FieldType type = getTypeCode(fieldType);
                fieldTypes[i] = type;
                switch (type) 
                {
                    case FieldType.tpObject:
                    case FieldType.tpLink:
                    case FieldType.tpArrayOfObject:
                        hasReferences = true;
                        break;
                    case FieldType.tpValue:
                        hasReferences |= new ClassDescriptor(fieldType).hasReferences;
                        break;
                    case FieldType.tpArrayOfValue:
                        hasReferences |= new ClassDescriptor(fieldType.GetElementType()).hasReferences;
                        break;
                }
            }
            defaultConstructor = cls.GetConstructor(BindingFlags.Instance|BindingFlags.Instance|BindingFlags.Public|BindingFlags.NonPublic|BindingFlags.DeclaredOnly, null, defaultConstructorProfile, null);
            if (defaultConstructor == null && !typeof(ValueType).IsAssignableFrom(cls)) 
            { 
                throw new StorageError(StorageError.ErrorCode.DESCRIPTOR_FAILURE, cls);
            }
        }
    }
}