package org.garret.perst.impl;
import  org.garret.perst.*;
import  java.lang.reflect.*;
import  java.util.ArrayList;

public final class ClassDescriptor extends Persistent { 
    ClassDescriptor next;
    String          name;
    int             nFields;

    transient Field[] allFields;
    transient int[]   fieldTypes;
    transient Class   cls;
    transient boolean hasSubclasses;
    transient Constructor defaultConstructor;

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
    public static final int tpLink             = 11;
    public static final int tpArrayOfBoolean   = 12;
    public static final int tpArrayOfByte      = 13;
    public static final int tpArrayOfChar      = 14;
    public static final int tpArrayOfShort     = 15;
    public static final int tpArrayOfInt       = 16;
    public static final int tpArrayOfLong      = 17;
    public static final int tpArrayOfFloat     = 18;
    public static final int tpArrayOfDouble    = 19;
    public static final int tpArrayOfString    = 20;
    public static final int tpArrayOfDate      = 21;
    public static final int tpArrayOfObject    = 22;

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

    static final Class[] defaultConstructorProfile = new Class[0];

    Persistent newInstance() {
        try { 
            return (Persistent)defaultConstructor.newInstance(null);
        } catch (Exception x) { 
            throw new StorageError(StorageError.CONSTRUCTOR_FAILURE, cls, x);
        }
    }

    void buildFieldList(Class cls, ArrayList list) throws Exception { 
        Class superclass = cls.getSuperclass();
        if (superclass != null) { 
            buildFieldList(superclass, list);
        }
        Field[] flds = cls.getDeclaredFields();
        for (int i = 0; i < flds.length; i++) { 
            Field f = flds[i];
            if ((f.getModifiers() & (Modifier.TRANSIENT|Modifier.STATIC)) == 0) {
                f.setAccessible(true);
                list.add(f);
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
        } else if (c.equals(Link.class)) {
            type = tpLink;
        } else if (c.isArray()) { 
            type = getTypeCode(c.getComponentType());
            if (type >= tpLink) { 
                throw new StorageError(StorageError.UNSUPPORTED_TYPE, c);
            }
            type += tpArrayOfBoolean;
        } else { 
            throw new StorageError(StorageError.UNSUPPORTED_TYPE, c);
        }
        return type;
    }

    ClassDescriptor() {}

    ClassDescriptor(Class cls) { 
        this.cls = cls;
        name = cls.getName();
        build();
        nFields = allFields.length;
    }

    public void resolve() {         
        if (cls == null) {
            try { 
                cls = Class.forName(name);
            } catch (ClassNotFoundException x) { 
                throw new StorageError(StorageError.CLASS_NOT_FOUND, name, x);
            }
            build();
            if (nFields != allFields.length) { 
                throw new StorageError(StorageError.SCHEMA_CHANGED, cls);
            }                
        }
    }

    void build() 
    {
        try { 
            ArrayList list = new ArrayList();
            buildFieldList(cls, list);
            int nFields = list.size();
            allFields = (Field[])list.toArray(new Field[nFields]);
            fieldTypes = new int[nFields];
            for (int i = 0; i < nFields; i++) { 
                fieldTypes[i] = getTypeCode(allFields[i].getType());
            }        
            defaultConstructor = cls.getDeclaredConstructor(defaultConstructorProfile);
            defaultConstructor.setAccessible(true);
        } catch (Exception x) {
            throw new StorageError(StorageError.DESCRIPTOR_FAILURE, cls, x);
        }
    }
}
