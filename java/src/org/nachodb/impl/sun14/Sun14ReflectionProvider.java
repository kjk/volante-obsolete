package org.nachodb.impl.sun14;

import java.lang.reflect.*;
import org.nachodb.impl.ReflectionProvider;
import java.util.*;
import sun.reflect.*;
import sun.misc.Unsafe;


public class Sun14ReflectionProvider implements ReflectionProvider { 
    private Constructor       javaLangObjectConstructor;
    private Unsafe            unsafe;
    private ReflectionFactory factory;
    private HashMap           constructorHash;

    static final Class[] defaultConstructorProfile = new Class[0];

    public Sun14ReflectionProvider() { 
        try { 
            Class objectStreamClass = Class.forName("java.io.ObjectStreamClass$FieldReflector");
            Field unsafeField = objectStreamClass.getDeclaredField("unsafe");
            unsafeField.setAccessible(true);
            unsafe = (Unsafe) unsafeField.get(null);
            javaLangObjectConstructor = Object.class.getDeclaredConstructor(new Class[0]);;
            factory = ReflectionFactory.getReflectionFactory();
            constructorHash = new HashMap();
        } catch (Exception x) {
            throw new Error("Failed to initialize reflection provider");
        }
    }

    public Constructor getDefaultConstructor(Class cls) throws Exception { 
        Constructor cons = (Constructor)constructorHash.get(cls);
        if (cons == null) { 
            try {             
                cons = cls.getDeclaredConstructor(defaultConstructorProfile);
            } catch (NoSuchMethodException x) {
                cons = factory.newConstructorForSerialization(cls, javaLangObjectConstructor);
            } 
            constructorHash.put(cls, cons);
        }
        return cons;
    }

    public void setInt(Field field, Object object, int value) throws Exception { 
        unsafe.putInt(object, unsafe.objectFieldOffset(field), value);
    }

    public void setLong(Field field, Object object, long value) throws Exception { 
        unsafe.putLong(object, unsafe.objectFieldOffset(field), value);
    }

    public void setShort(Field field, Object object, short value) throws Exception { 
        unsafe.putShort(object, unsafe.objectFieldOffset(field), value);
    }

    public void setChar(Field field, Object object, char value) throws Exception { 
        unsafe.putChar(object, unsafe.objectFieldOffset(field), value);
    }

    public void setByte(Field field, Object object, byte value) throws Exception { 
        unsafe.putByte(object, unsafe.objectFieldOffset(field), value);
    }

    public void setFloat(Field field, Object object, float value) throws Exception { 
        unsafe.putFloat(object, unsafe.objectFieldOffset(field), value);
    }

    public void setDouble(Field field, Object object, double value) throws Exception { 
        unsafe.putDouble(object, unsafe.objectFieldOffset(field), value);
    }

    public void setBoolean(Field field, Object object, boolean value) throws Exception { 
        unsafe.putBoolean(object, unsafe.objectFieldOffset(field), value);
    }

    public void set(Field field, Object object, Object value) throws Exception { 
        unsafe.putObject(object, unsafe.objectFieldOffset(field), value);
    }
}