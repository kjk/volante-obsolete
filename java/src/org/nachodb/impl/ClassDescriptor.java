package org.nachodb.impl;
import  org.nachodb.*;
import  java.lang.reflect.*;
import  java.util.*;

public final class ClassDescriptor extends Persistent { 
    ClassDescriptor   next;
    String            name;
    boolean           hasReferences;
    FieldDescriptor[] allFields;

    static class FieldDescriptor extends Persistent { 
        String          fieldName;
        String          className;
        int             type;
        ClassDescriptor valueDesc;
        transient Field field;

        public boolean equals(FieldDescriptor fd) { 
            return fieldName.equals(fd.fieldName) 
                && className.equals(fd.className)
                && valueDesc == fd.valueDesc
                && type == fd.type;
        }
    }    

    transient Class       cls;
    transient Constructor loadConstructor;
    transient LoadFactory factory;
    transient Object[]    constructorParams;
    transient boolean     hasSubclasses;
    transient boolean     resolved;
    
    static ReflectionProvider reflectionProvider; 

    public static final int tpBoolean          = 0;
    public static final int tpByte             = 1;
    public static final int tpChar             = 2;
    public static final int tpShort            = 3;
    public static final int tpInt              = 4;
    public static final int tpLong             = 5;
    public static final int tpFloat            = 6;
    public static final int tpDouble           = 7;
    public static final int tpString           = 8;
    public static final int tpDate             = 9;
    public static final int tpObject           = 10;
    public static final int tpValue            = 11;
    public static final int tpRaw              = 12;
    public static final int tpLink             = 13;
    public static final int tpArrayOfBoolean   = 20;
    public static final int tpArrayOfByte      = 21;
    public static final int tpArrayOfChar      = 22;
    public static final int tpArrayOfShort     = 23;
    public static final int tpArrayOfInt       = 24;
    public static final int tpArrayOfLong      = 25;
    public static final int tpArrayOfFloat     = 26;
    public static final int tpArrayOfDouble    = 27;
    public static final int tpArrayOfString    = 28;
    public static final int tpArrayOfDate      = 29;
    public static final int tpArrayOfObject    = 30;
    public static final int tpArrayOfValue     = 31;
    public static final int tpArrayOfRaw       = 32;

    static final String signature[] = {
        "boolean", 
        "byte",
        "char",
        "short",
        "int",
        "long",
        "float",
        "double",
        "String",
        "Date",
        "Object",
        "Value",
        "Raw",
        "Link",
        "", 
        "", 
        "", 
        "", 
        "", 
        "", 
        "ArrayOfBoolean",
        "ArrayOfByte",
        "ArrayOfChar",
        "ArrayOfShort",
        "ArrayOfInt",
        "ArrayOfLong",
        "ArrayOfFloat",
        "ArrayOfDouble",
        "ArrayOfString",
        "ArrayOfDate",
        "ArrayOfObject",
        "ArrayOfValue",
        "ArrayOfRaw"
    };
        

    static final int sizeof[] = {
        1, // tpBoolean
        1, // tpByte
        2, // tpChar
        2, // tpShort
        4, // tpInt
        8, // tpLong
        4, // tpFloat
        8, // tpDouble
        0, // tpString
        8, // tpDate
        4  // tpObject
    };

    static final Class[] perstConstructorProfile = new Class[]{ClassDescriptor.class};

    static ReflectionProvider getReflectionProvider() { 
        if (reflectionProvider == null) { 
            try {
                Class.forName("sun.misc.Unsafe");
                String cls = "org.nachodb.impl.sun14.Sun14ReflectionProvider";
                reflectionProvider = (ReflectionProvider)Class.forName(cls).newInstance();
            } catch (Exception x) { 
                reflectionProvider = new StandardReflectionProvider();
            }
        }
        return reflectionProvider;
    } 
           

    public boolean equals(ClassDescriptor cd) { 
        if (cd == null || allFields.length != cd.allFields.length) { 
            return false;
        }
        for (int i = 0; i < allFields.length; i++) { 
            if (!allFields[i].equals(cd.allFields[i])) { 
                return false;
            }
        }
        return true;
    }
        

    Object newInstance() {
        if (factory != null) { 
            return factory.create(this);
        } else { 
            try { 
                return loadConstructor.newInstance(constructorParams);
            } catch (Exception x) { 
                throw new StorageError(StorageError.CONSTRUCTOR_FAILURE, cls, x);
            }
        }
    }

    void buildFieldList(StorageImpl storage, Class cls, ArrayList list) { 
        Class superclass = cls.getSuperclass();
        if (superclass != null) { 
            buildFieldList(storage, superclass, list);
        }
        Field[] flds = cls.getDeclaredFields();
        for (int i = 0; i < flds.length; i++) { 
            Field f = flds[i];
            if ((f.getModifiers() & (Modifier.TRANSIENT|Modifier.STATIC)) == 0) {
                f.setAccessible(true);
                FieldDescriptor fd = new FieldDescriptor();
                fd.field = f;
                fd.fieldName = f.getName();
                fd.className = cls.getName();
                int type = getTypeCode(f.getType());
                switch (type) {
                  case tpObject:
                  case tpLink:
                  case tpArrayOfObject:
                    hasReferences = true;
                    break;
                  case tpValue:
                    fd.valueDesc = storage.getClassDescriptor(f.getType());
                    hasReferences |= fd.valueDesc.hasReferences;                    
                    break;
                  case tpArrayOfValue:
                    fd.valueDesc = storage.getClassDescriptor(f.getType().getComponentType());
                    hasReferences |= fd.valueDesc.hasReferences;
                }
                fd.type = type;
                list.add(fd);
            }
        }
    }

    public static int getTypeCode(Class c) { 
        int type;
        if (c.equals(byte.class)) { 
            type = tpByte;
        } else if (c.equals(short.class)) {
            type = tpShort;
        } else if (c.equals(char.class)) {
            type = tpChar;
        } else if (c.equals(int.class)) {
            type = tpInt;
        } else if (c.equals(long.class)) {
            type = tpLong;
        } else if (c.equals(float.class)) {
            type = tpFloat;
        } else if (c.equals(double.class)) {
            type = tpDouble;
        } else if (c.equals(String.class)) {
            type = tpString;
        } else if (c.equals(boolean.class)) {
            type = tpBoolean;
        } else if (c.equals(java.util.Date.class)) {
            type = tpDate;
        } else if (IPersistent.class.isAssignableFrom(c)) {
            type = tpObject;
        } else if (IValue.class.isAssignableFrom(c)) {
            type = tpValue;
        } else if (c.equals(Link.class)) {
            type = tpLink;
        } else if (c.isArray()) { 
            type = getTypeCode(c.getComponentType());
            if (type >= tpLink) { 
                throw new StorageError(StorageError.UNSUPPORTED_TYPE, c);
            }
            type += tpArrayOfBoolean;
        } else if (c.equals(Object.class) || c.equals(Comparable.class)) {
            type = tpRaw;
        } else if (serializeNonPersistentObjects) {
            type = tpRaw;            
        } else if (treateAnyNonPersistentObjectAsValue) {
            if (c.equals(Object.class)) { 
                throw new StorageError(StorageError.EMPTY_VALUE);
            }
            type = tpValue;            
        } else { 
            throw new StorageError(StorageError.UNSUPPORTED_TYPE, c);
        }
        return type;
    }

    static boolean treateAnyNonPersistentObjectAsValue = Boolean.getBoolean("perst.implicit.values");
    static boolean serializeNonPersistentObjects = Boolean.getBoolean("perst.serialize.transient.objects");

    ClassDescriptor() {}


    private void locateConstructor() { 
        try { 
            //Class c = Class.forName(cls.getName() + "LoadFactory");
            Class c = Thread.currentThread().getContextClassLoader().loadClass(cls.getName() + "LoadFactory");
            factory = (LoadFactory)c.newInstance();
        } catch (Exception x1) { 
            try {             
                loadConstructor = cls.getDeclaredConstructor(perstConstructorProfile);
                constructorParams = new Object[]{this};
            } catch (NoSuchMethodException x2) {
                try { 
                    loadConstructor = getReflectionProvider().getDefaultConstructor(cls);
                    constructorParams = null;
                } catch (Exception x3) {
                    throw new StorageError(StorageError.DESCRIPTOR_FAILURE, cls, x3);
                }
            }
            loadConstructor.setAccessible(true);
        }
    }

    ClassDescriptor(StorageImpl storage, Class cls) { 
        this.cls = cls;
        name = cls.getName();
        ArrayList list = new ArrayList();
        buildFieldList(storage, cls, list);
        allFields = (FieldDescriptor[])list.toArray(new FieldDescriptor[list.size()]);
        locateConstructor();
        resolved = true;
    }

    protected static Class loadClass(Storage storage, String name) { 
        ClassLoader loader = storage.getClassLoader();
        if (loader != null) { 
            try { 
                return loader.loadClass(name);
            } catch (ClassNotFoundException x) {}
        }
        try { 
            return Thread.currentThread().getContextClassLoader().loadClass(name);
            // return Class.forName(name);
        } catch (ClassNotFoundException x) { 
            throw new StorageError(StorageError.CLASS_NOT_FOUND, name, x);
        }
    }

    public void onLoad() {         
        cls = loadClass(getStorage(), name);
        Class scope = cls;
        int n = allFields.length;
        for (int i = n; --i >= 0;) { 
            FieldDescriptor fd = allFields[i];
            fd.load();
            if (!fd.className.equals(scope.getName())) {
                for (scope = cls; scope != null; scope = scope.getSuperclass()) { 
                    if (fd.className.equals(scope.getName())) {
                        break;
                    }
                }
            }
            if (scope != null) {
                try { 
                    Field f = scope.getDeclaredField(fd.fieldName);
                    if ((f.getModifiers() & (Modifier.TRANSIENT|Modifier.STATIC)) == 0) {
                        f.setAccessible(true);
                        fd.field = f;
                    }
                } catch (NoSuchFieldException x) {}
            } else { 
                scope = cls;
            }
        }
        for (int i = n; --i >= 0;) { 
            FieldDescriptor fd = allFields[i];
            if (fd.field == null) { 
            hierarchyLoop:
                for (scope = cls; scope != null; scope = scope.getSuperclass()) { 
                    try { 
                        Field f = scope.getDeclaredField(fd.fieldName);
                        if ((f.getModifiers() & (Modifier.TRANSIENT|Modifier.STATIC)) == 0) {
                            for (int j = 0; j < n; j++) { 
                                if (allFields[j].field == f) { 
                                    continue hierarchyLoop;
                                }
                            }
                            f.setAccessible(true);
                            fd.field = f;
                            break;
                        }
                    } catch (NoSuchFieldException x) {}
                }
            }
        }
        locateConstructor();
        StorageImpl s = (StorageImpl)getStorage();
        if (s.classDescMap.get(cls) == null) { 
            s.classDescMap.put(cls, this);
        }
    }

       
    void resolve() {
        if (!resolved) { 
            StorageImpl classStorage = (StorageImpl)getStorage();
            ClassDescriptor desc = new ClassDescriptor(classStorage, cls);
            resolved = true;
            if (!desc.equals(this)) { 
                classStorage.registerClassDescriptor(desc);
            }
        }
    }            

    public boolean recursiveLoading() { 
        return false;
    }
}
