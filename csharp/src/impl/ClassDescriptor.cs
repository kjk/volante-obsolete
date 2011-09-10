namespace Volante.Impl
{
    using System;
    using System.Collections;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.Diagnostics;
    using System.Text;
    using Volante;

    public sealed class ClassDescriptor : Persistent
    {
        internal ClassDescriptor next;
        internal String name;
        internal FieldDescriptor[] allFields;
        internal bool hasReferences;
        internal static Module lastModule;

        public class FieldDescriptor : Persistent
        {
            internal String fieldName;
            internal String className;
            internal FieldType type;
            internal ClassDescriptor valueDesc;
            [NonSerialized()]
            internal FieldInfo field;
            [NonSerialized()]
            internal bool recursiveLoading;
            [NonSerialized()]
            internal MethodInfo constructor;

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
            tpArrayOfOid,
            tpArrayOfValue,
            tpArrayOfRaw,
            tpArrayOfGuid,
            tpArrayOfDecimal,
            tpLast
        }

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

        internal static Type[] defaultConstructorProfile = new Type[0];
        internal static object[] noArgs = new object[0];

#if CF
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
                throw new DatabaseException(DatabaseException.ErrorCode.CONSTRUCTOR_FAILURE, cls, x);
            }
        }

#if CF
        internal void generateSerializer() {}
#else
        private static CodeGenerator serializerGenerator = CodeGenerator.Instance;

        internal void generateSerializer()
        {
            if (!cls.IsPublic || defaultConstructor == null || !defaultConstructor.IsPublic)
                return;

            FieldDescriptor[] flds = allFields;
            for (int i = 0, n = flds.Length; i < n; i++)
            {
                FieldDescriptor fd = flds[i];
                switch (fd.type)
                {
                    case FieldType.tpValue:
                    case FieldType.tpArrayOfValue:
                    case FieldType.tpArrayOfObject:
                    case FieldType.tpArrayOfEnum:
                    case FieldType.tpArrayOfRaw:
                    case FieldType.tpLink:
                    case FieldType.tpArrayOfOid:
                        return;
                    default:
                        break;
                }
                FieldInfo f = flds[i].field;
                if (f == null || !f.IsPublic)
                    return;
            }
            serializer = serializerGenerator.Generate(this);
        }

        static private bool isObjectProperty(Type cls, FieldInfo f)
        {
            return typeof(PersistentWrapper).IsAssignableFrom(cls) && f.Name.StartsWith("r_");
        }
#endif

        MethodInfo GetConstructor(FieldInfo f, string name)
        {
            MethodInfo mi = typeof(DatabaseImpl).GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.DeclaredOnly);
            //return mi.BindGenericParameters(f.FieldType.GetGenericArguments());
            //TODO: verify it's MakeGenericMethod
            return mi.MakeGenericMethod(f.FieldType.GetGenericArguments());
        }

        internal static String getTypeName(Type t)
        {
            if (t.IsGenericType)
            {
                Type[] genericArgs = t.GetGenericArguments();
                t = t.GetGenericTypeDefinition();
                StringBuilder buf = new StringBuilder(t.FullName);
                buf.Append('=');
                char sep = '[';
                for (int j = 0; j < genericArgs.Length; j++)
                {
                    buf.Append(sep);
                    sep = ',';
                    buf.Append(getTypeName(genericArgs[j]));
                }
                buf.Append(']');
                return buf.ToString();
            }
            return t.FullName;
        }

        static bool isVolanteInternalType(Type t)
        {
            return t.Namespace == typeof(IPersistent).Namespace
                && t != typeof(IPersistent) && t != typeof(PersistentContext) && t != typeof(Persistent);
        }

        internal void buildFieldList(DatabaseImpl db, System.Type cls, ArrayList list)
        {
            System.Type superclass = cls.BaseType;
            if (superclass != null && superclass != typeof(MarshalByRefObject))
            {
                buildFieldList(db, superclass, list);
            }
            System.Reflection.FieldInfo[] flds = cls.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.DeclaredOnly);
#if !CF
            bool isWrapper = typeof(PersistentWrapper).IsAssignableFrom(cls);
#endif
            bool hasTransparentAttribute = cls.GetCustomAttributes(typeof(TransparentPersistenceAttribute), true).Length != 0;

            for (int i = 0; i < flds.Length; i++)
            {
                FieldInfo f = flds[i];
                if (!f.IsNotSerialized && !f.IsStatic)
                {
                    FieldDescriptor fd = new FieldDescriptor();
                    fd.field = f;
                    fd.fieldName = f.Name;
                    fd.className = getTypeName(cls);
                    Type fieldType = f.FieldType;
                    FieldType type = getTypeCode(fieldType);
                    switch (type)
                    {
#if !CF
                        case FieldType.tpInt:
                            if (isWrapper && isObjectProperty(cls, f))
                            {
                                hasReferences = true;
                                type = FieldType.tpOid;
                            }
                            break;
#endif
                        case FieldType.tpArrayOfOid:
                            fd.constructor = GetConstructor(f, "ConstructArray");
                            hasReferences = true;
                            break;
                        case FieldType.tpLink:
                            fd.constructor = GetConstructor(f, "ConstructLink");
                            hasReferences = true;
                            break;

                        case FieldType.tpArrayOfObject:
                        case FieldType.tpObject:
                            hasReferences = true;
                            if (hasTransparentAttribute && isVolanteInternalType(fieldType))
                            {
                                fd.recursiveLoading = true;
                            }
                            break;
                        case FieldType.tpValue:
                            fd.valueDesc = db.getClassDescriptor(f.FieldType);
                            hasReferences |= fd.valueDesc.hasReferences;
                            break;
                        case FieldType.tpArrayOfValue:
                            fd.valueDesc = db.getClassDescriptor(f.FieldType.GetElementType());
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
            else if (typeof(IGenericPArray).IsAssignableFrom(c))
            {
                type = FieldType.tpArrayOfOid;
            }
            else if (typeof(IGenericLink).IsAssignableFrom(c))
            {
                type = FieldType.tpLink;
            }
            else if (c.IsArray)
            {
                type = getTypeCode(c.GetElementType());
                if ((int)type >= (int)FieldType.tpLink)
                {
                    throw new DatabaseException(DatabaseException.ErrorCode.UNSUPPORTED_TYPE, c);
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

        internal ClassDescriptor(DatabaseImpl db, Type cls)
        {
            this.cls = cls;
            name = getTypeName(cls);
            ArrayList list = new ArrayList();
            buildFieldList(db, cls, list);
            allFields = (FieldDescriptor[])list.ToArray(typeof(FieldDescriptor));
            defaultConstructor = cls.GetConstructor(BindingFlags.Instance | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly, null, defaultConstructorProfile, null);
            if (defaultConstructor == null && !typeof(ValueType).IsAssignableFrom(cls))
                throw new DatabaseException(DatabaseException.ErrorCode.DESCRIPTOR_FAILURE, cls);
            resolved = true;
        }

        internal static Type lookup(IDatabase db, String name)
        {
            var resolvedTypes = ((DatabaseImpl)db).resolvedTypes;
            lock (resolvedTypes)
            {
                Type cls;
                var ok = resolvedTypes.TryGetValue(name, out cls);
                if (ok)
                    return cls;
                IClassLoader loader = db.Loader;
                if (loader != null)
                {
                    cls = loader.LoadClass(name);
                    if (cls != null)
                    {
                        resolvedTypes[name] = cls;
                        return cls;
                    }
                }
                Module last = lastModule;
                if (last != null)
                {
                    cls = last.GetType(name);
                    if (cls != null)
                    {
                        resolvedTypes[name] = cls;
                        return cls;
                    }
                }

                int p = name.IndexOf('=');
                if (p >= 0)
                {
                    Type genericType = lookup(db, name.Substring(0, p));
                    Type[] genericParams = new Type[genericType.GetGenericArguments().Length];
                    int nest = 0;
                    int i = p += 2;
                    int n = 0;

                    while (true)
                    {
                        switch (name[i++])
                        {
                            case '[':
                                nest += 1;
                                break;
                            case ']':
                                if (--nest < 0)
                                {
                                    genericParams[n++] = lookup(db, name.Substring(p, i - p - 1));
                                    Debug.Assert(n == genericParams.Length);
                                    cls = genericType.MakeGenericType(genericParams);
                                    if (cls == null)
                                    {
                                        throw new DatabaseException(DatabaseException.ErrorCode.CLASS_NOT_FOUND, name);
                                    }
                                    resolvedTypes[name] = cls;
                                    return cls;
                                }
                                break;
                            case ',':
                                if (nest == 0)
                                {
                                    genericParams[n++] = lookup(db, name.Substring(p, i - p - 1));
                                    p = i;
                                }
                                break;
                        }
                    }
                }

#if CF
                foreach (Assembly ass in DatabaseImpl.assemblies) 
#else
                foreach (Assembly ass in AppDomain.CurrentDomain.GetAssemblies())
#endif
                {
                    foreach (Module mod in ass.GetModules())
                    {
                        Type t = mod.GetType(name);
                        if (t != null)
                        {
                            if (cls != null)
                            {
                                throw new DatabaseException(DatabaseException.ErrorCode.AMBIGUITY_CLASS, name);
                            }
                            else
                            {
                                lastModule = mod;
                                cls = t;
                            }
                        }
                    }
                }
#if !CF
                if (cls == null && name.EndsWith("Wrapper"))
                {
                    Type originalType = lookup(db, name.Substring(0, name.Length - 7));
                    lock (db)
                    {
                        cls = ((DatabaseImpl)db).getWrapper(originalType);
                    }
                }
#endif
                if (cls == null)
                {
                    throw new DatabaseException(DatabaseException.ErrorCode.CLASS_NOT_FOUND, name);
                }
                resolvedTypes[name] = cls;
                return cls;
            }
        }

        public override void OnLoad()
        {
            cls = lookup(Database, name);
            int n = allFields.Length;
            bool hasTransparentAttribute = cls.GetCustomAttributes(typeof(TransparentPersistenceAttribute), true).Length != 0;
            for (int i = n; --i >= 0; )
            {
                FieldDescriptor fd = allFields[i];
                fd.Load();
                fd.field = cls.GetField(fd.fieldName, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
                if (hasTransparentAttribute && fd.type == FieldType.tpObject && isVolanteInternalType(fd.field.FieldType))
                {
                    fd.recursiveLoading = true;
                }

                switch (fd.type)
                {
                    case FieldType.tpArrayOfOid:
                        fd.constructor = GetConstructor(fd.field, "ConstructArray");
                        break;
                    case FieldType.tpLink:
                        fd.constructor = GetConstructor(fd.field, "ConstructLink");
                        break;
                    default:
                        break;
                }
            }

            defaultConstructor = cls.GetConstructor(BindingFlags.Instance | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly, null, defaultConstructorProfile, null);
            if (defaultConstructor == null && !typeof(ValueType).IsAssignableFrom(cls))
            {
                throw new DatabaseException(DatabaseException.ErrorCode.DESCRIPTOR_FAILURE, cls);
            }
            DatabaseImpl s = (DatabaseImpl)Database;
            if (!s.classDescMap.ContainsKey(cls))
            {
                ((DatabaseImpl)Database).classDescMap.Add(cls, this);
            }
        }

        internal void resolve()
        {
            if (resolved)
                return;

            DatabaseImpl classStorage = (DatabaseImpl)Database;
            ClassDescriptor desc = new ClassDescriptor(classStorage, cls);
            resolved = true;
            if (!desc.equals(this))
            {
                classStorage.registerClassDescriptor(desc);
            }
        }

        public override bool RecursiveLoading()
        {
            return false;
        }
    }
}