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
        internal static Module     lastModule;

        public class FieldDescriptor : Persistent 
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
        [NonSerialized()]
        internal GeneratedSerializer serializer;
		
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
            tpOid,
            tpValue,
            tpRaw,
            tpGuid,
            tpDecimal,
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
            tpArrayOfOid, // not used
            tpArrayOfValue,
            tpArrayOfRaw,
            tpArrayOfGuid,
            tpArrayOfDecimal,
            tpLast
        };
		
        internal static int[] Sizeof = new int[] 
        {
            1, // tpBoolean,
            1, // tpByte,
            1, // tpSByte,
            2, // tpShort, 
            2, // tpUShort,
            2, // tpChar,
            4, // tpEnum,
            4, // tpInt,
            4, // tpUInt,
            8, // tpLong,
            8, // tpULong,
            4, // tpFloat,
            8, // tpDouble,
            0, // tpString,
            8, // tpDate,
            4, // tpObject,
            4, // tpOid,
            0, // tpValue,
            0, // tpRaw,
            16,// tpGuid,
            16,// tpDecimal,
            0, // tpLink,
            0, // tpArrayOfBoolean,
            0, // tpArrayOfByte,
            0, // tpArrayOfSByte,
            0, // tpArrayOfShort, 
            0, // tpArrayOfUShort,
            0, // tpArrayOfChar,
            0, // tpArrayOfEnum,
            0, // tpArrayOfInt,
            0, // tpArrayOfUInt,
            0, // tpArrayOfLong,
            0, // tpArrayOfULong,
            0, // tpArrayOfFloat,
            0, // tpArrayOfDouble,
            0, // tpArrayOfString,
            0, // tpArrayOfDate,
            0, // tpArrayOfObject,
            0, // tpArrayOfOid,
            0, // tpArrayOfValue,
            0, // tpArrayOfRaw,
            0, // tpArrayOfGuid,
            0  // tpArrayOfDecimal,
        };
		
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
		
#if COMPACT_NET_FRAMEWORK
        internal void generateSerializer() {}
#else
        private static CodeGenerator serializerGenerator = CodeGenerator.Instance;

        internal void generateSerializer()
        {
            if (!cls.IsPublic || defaultConstructor == null || !defaultConstructor.IsPublic) 
            { 
                return;
            }
            FieldDescriptor[] flds = allFields;
            for (int i = 0, n = flds.Length; i < n; i++) 
            {
                FieldDescriptor fd = flds[i];
                switch (fd.type) 
                { 
                    case FieldType.tpValue:
                    case FieldType.tpArrayOfValue:
                    case FieldType.tpArrayOfObject:
                    case FieldType.tpArrayOfOid:
                    case FieldType.tpArrayOfEnum:
                    case FieldType.tpArrayOfRaw:
                        return;
                    default:
                        break;
                }
                FieldInfo f = flds[i].field;
                if (f == null || !f.IsPublic) 
                {
                    return;
                }
            }
            serializer = serializerGenerator.Generate(this);
        }
        
        static private bool isObjectProperty(Type cls, FieldInfo f)
        {
            return typeof(PersistentWrapper).IsAssignableFrom(cls) && f.Name.StartsWith("r_");
         }
#endif

        internal void  buildFieldList(StorageImpl storage, System.Type cls, ArrayList list)
        {
            System.Type superclass = cls.BaseType;
            if (superclass != null && superclass != typeof(MarshalByRefObject))
            {
                buildFieldList(storage, superclass, list);
            }
            System.Reflection.FieldInfo[] flds = cls.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.DeclaredOnly);
#if !COMPACT_NET_FRAMEWORK 
            bool isWrapper = typeof(PersistentWrapper).IsAssignableFrom(cls);
#endif
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
#if !COMPACT_NET_FRAMEWORK 
                        case FieldType.tpInt:
                            if (isWrapper && isObjectProperty(cls, f)) 
                            {
                                hasReferences = true;
                                type = FieldType.tpOid;
                            } 
                            break;
#endif
                        case FieldType.tpArrayOfOid:
                        case FieldType.tpArrayOfObject:
                        case FieldType.tpObject:
                        case FieldType.tpLink:
                            hasReferences = true;
                            break;
                        case FieldType.tpValue:
                            fd.valueDesc = storage.getClassDescriptor(f.FieldType);
                            hasReferences |= fd.valueDesc.hasReferences;
                            break;
                        case FieldType.tpArrayOfValue:
                            fd.valueDesc = storage.getClassDescriptor(f.FieldType.GetElementType());
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
            else if (c.Equals(typeof(decimal)))
            { 
                type = FieldType.tpDecimal;
            }
            else if (c.Equals(typeof(Guid))) 
            { 
                type = FieldType.tpGuid;
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
                if (serializeNonPersistentObjects || c == typeof(object) || c == typeof(IComparable)) 
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

        internal static Type lookup(Storage storage, String name)
        {
            Type cls = null;
            ClassLoader loader = storage.Loader;
            if (loader != null) 
            { 
                cls = loader.LoadClass(name);
                if (cls != null) 
                { 
                    return cls;
                }
            }
            Module last = lastModule;
            if (last != null) 
            {
                Type t = last.GetType(name);
                if (t != null) 
                {
                    return t;
                }
            }
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
                            lastModule = mod;
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
                            lastModule = mod;
                            cls = t;
                        }
                    }
                }
            }
            if (cls == null && name.EndsWith("Wrapper")) 
            {
                Type originalType = lookup(storage, name.Substring(0, name.Length-7));
                lock (storage) 
                {
                    return ((StorageImpl)storage).getWrapper(originalType);
                }
            }
#endif
            if (cls == null) 
            {
                throw new StorageError(StorageError.ErrorCode.CLASS_NOT_FOUND, name);
            }
            return cls;
        }

        public override void OnLoad()
        {
            cls = lookup(Storage, name);
            Type scope = cls;
            int n = allFields.Length;
            for (int i = n; --i >= 0;) 
            { 
                FieldDescriptor fd = allFields[i];
                fd.Load();
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
            StorageImpl s = (StorageImpl)Storage;
            if (!s.classDescMap.Contains(cls)) 
            {
                ((StorageImpl)Storage).classDescMap.Add(cls, this);
            }
        }

        internal void resolve() 
        {
            if (!resolved) 
            { 
                StorageImpl classStorage = (StorageImpl)Storage;
                ClassDescriptor desc = new ClassDescriptor(classStorage, cls);
                resolved = true;
                if (!desc.equals(this)) 
                { 
                    classStorage.registerClassDescriptor(desc);
                }
            }
        }            

        public override bool RecursiveLoading()
        {
            return false;
        }
    }
}