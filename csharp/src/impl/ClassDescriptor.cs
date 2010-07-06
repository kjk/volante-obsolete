namespace Perst.Impl
{
    using System;
    using System.Collections;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using Perst;
	
    public sealed class ClassDescriptor:Persistent
    {
        internal ClassDescriptor   next;
        internal String            name;
        internal FieldDescriptor[] allFields;
        internal bool              hasReferences;

        internal class FieldDescriptor : Persistent 
        { 
            internal String          fieldName;
            internal String          className;
            internal FieldType       type;
            internal ClassDescriptor valueDesc;
            [NonSerialized()]
            internal FieldInfo       field;

            public bool equals(FieldDescriptor fd) 
            { 
                return fieldName.Equals(fd.fieldName) 
                    && className.Equals(fd.className)
                    && valueDesc == fd.valueDesc
                    && type == fd.type;
            }
        }    
        [NonSerialized()]
        internal Type cls;
        [NonSerialized()]
        internal bool hasSubclasses;
        [NonSerialized()]
        internal ConstructorInfo defaultConstructor;
        [NonSerialized()]
        internal bool resolved;
		
        internal static bool serializeNonPersistentObjects;

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
#if SUPPORT_RAW_TYPE
            tpRaw,
#endif
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
            tpArrayOfValue
#if SUPPORT_RAW_TYPE
                ,tpArrayOfRaw
#endif
        };
		
        internal static int[] Sizeof = new int[] {1, 1, 1, 2, 2, 2, 4, 4, 4, 8, 8, 4, 8, 0, 8, 4};
		
        internal static System.Type[] defaultConstructorProfile = new System.Type[0];
        internal static System.Object[] noArgs = new System.Object[0];
	
	
#if COMPACT_NET_FRAMEWORK
        static internal object parseEnum(Type type, String value) 
        {
            foreach (FieldInfo fi in type.GetFields()) 
            {
                if (fi.IsLiteral && fi.Name.Equals(value)) 
                {
                    return fi.GetValue(null);
                }
            }
            throw new ArgumentException(value);
        }
#endif

        public bool equals(ClassDescriptor cd) 
        { 
            if (cd == null || allFields.Length != cd.allFields.Length) 
            { 
                return false;
            }
            for (int i = 0; i < allFields.Length; i++) 
            { 
                if (!allFields[i].equals(cd.allFields[i])) 
                { 
                    return false;
                }
            }
            return true;
        }
        
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
		
        internal void  buildFieldList(StorageImpl storage, System.Type cls, ArrayList list)
        {
            System.Type superclass = cls.BaseType;
            if (superclass != null)
            {
                buildFieldList(storage, superclass, list);
            }
            System.Reflection.FieldInfo[] flds = cls.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.DeclaredOnly);
            for (int i = 0; i < flds.Length; i++)
            {
                FieldInfo f = flds[i];
                if (!f.IsNotSerialized && !f.IsStatic)
                {
                    FieldDescriptor fd = new FieldDescriptor();
                    fd.field = f;
                    fd.fieldName = f.Name;
                    fd.className = cls.FullName;
                    FieldType type = getTypeCode(f.FieldType);
                    switch (type) 
                    {
                        case FieldType.tpObject:
                        case FieldType.tpLink:
                        case FieldType.tpArrayOfObject:
                            hasReferences = true;
                            break;
                        case FieldType.tpValue:
                            fd.valueDesc = storage.getClassDescriptor(f.FieldType).resolve();
                            hasReferences |= fd.valueDesc.hasReferences;
                            break;
                        case FieldType.tpArrayOfValue:
                            fd.valueDesc = storage.getClassDescriptor(f.FieldType.GetElementType()).resolve();
                            hasReferences |= fd.valueDesc.hasReferences;
                            break;
                    }
                    fd.type = type;
                    list.Add(fd);
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
#if SUPPORT_RAW_TYPE
                if (serializeNonPersistentObjects) 
                {
                    type = FieldType.tpRaw;
                } 
                else 
                { 
                    throw new StorageError(StorageError.ErrorCode.UNSUPPORTED_TYPE, c);
                }
#else
                throw new StorageError(StorageError.ErrorCode.UNSUPPORTED_TYPE, c);
#endif
            }
            return type;
        }
		
        internal ClassDescriptor()
        {
        }
		
        internal ClassDescriptor(StorageImpl storage, Type cls)
        {
            this.cls = cls;
            name = cls.FullName;
            ArrayList list = new ArrayList();
            buildFieldList(storage, cls, list);
            allFields = (FieldDescriptor[]) list.ToArray(typeof(FieldDescriptor));
            defaultConstructor = cls.GetConstructor(BindingFlags.Instance|BindingFlags.Instance|BindingFlags.Public|BindingFlags.NonPublic|BindingFlags.DeclaredOnly, null, defaultConstructorProfile, null);
            if (defaultConstructor == null && !typeof(ValueType).IsAssignableFrom(cls)) 
            { 
                throw new StorageError(StorageError.ErrorCode.DESCRIPTOR_FAILURE, cls);
            }
            resolved = true;
        }
		
        internal static bool FindTypeByName(Type t, object name) 
        { 
            return t.FullName.Equals(name);
        }

        internal static Type lookup(String name)
        {
            Type cls = null;
#if COMPACT_NET_FRAMEWORK
            foreach (Assembly ass in StorageImpl.assemblies) 
            { 
                foreach (Module mod in ass.GetModules()) 
                { 
                    Type t = mod.GetType(name);
                    if (t != null) 
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
#else
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
#endif
            if (cls == null) 
            {
                throw new StorageError(StorageError.ErrorCode.CLASS_NOT_FOUND, name);
            }
            return cls;
        }

        public override void onLoad()
        {
            cls = lookup(name);
            Type scope = cls;
            int n = allFields.Length;
            for (int i = n; --i >= 0;) 
            { 
                FieldDescriptor fd = allFields[i];
                if (!fd.className.Equals(scope.FullName)) 
                {
                    for (scope = cls; scope != null; scope = scope.BaseType) 
                    { 
                        if (fd.className.Equals(scope.FullName)) 
                        {
                            break;
                        }
                    }
                }
                if (scope != null) 
                {
                    fd.field = scope.GetField(fd.fieldName, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.DeclaredOnly);
                } 
                else 
                { 
                    scope = cls;
                }
            }
            for (int i = n; --i >= 0;) 
            { 
                FieldDescriptor fd = allFields[i];
                if (fd.field == null) 
                { 
                
                    for (scope = cls; scope != null; scope = scope.BaseType) 
                    { 
                        FieldInfo f = scope.GetField(fd.fieldName, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.DeclaredOnly);
                        if (f != null) 
                        { 
                            for (int j = 0; j < n; j++) 
                            { 
                                if (allFields[j].field == f) 
                                { 
                                    goto hierarchyLoop;
                                }
                            }
                            fd.field = f;
                            break;
                        }
                        hierarchyLoop:;
                    }
                }
            }
            defaultConstructor = cls.GetConstructor(BindingFlags.Instance|BindingFlags.Instance|BindingFlags.Public|BindingFlags.NonPublic|BindingFlags.DeclaredOnly, null, defaultConstructorProfile, null);
            if (defaultConstructor == null && !typeof(ValueType).IsAssignableFrom(cls)) 
            { 
                throw new StorageError(StorageError.ErrorCode.DESCRIPTOR_FAILURE, cls);
            }
            ((StorageImpl)getStorage()).classDescMap[cls] = this;
        }

        internal ClassDescriptor resolve() 
        {
            if (!resolved) 
            { 
                StorageImpl classStorage = (StorageImpl)storage;
                ClassDescriptor desc = new ClassDescriptor(classStorage, cls);
                if (!desc.equals(this)) 
                { 
                    classStorage.registerClassDescriptor(desc);
                    return desc;
                }
                resolved = true;
            }
            return this;
        }            
    }
}