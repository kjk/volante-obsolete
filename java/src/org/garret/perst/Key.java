package org.garret.perst;
import  org.garret.perst.impl.ClassDescriptor;

/**
 * Class for specifying key value (neededd to access obejct by key usig index)
 */
public class Key { 
    public final int     type;

    public final int     ival;
    public final long    lval;
    public final double  dval;
    public final char[]  sval;

    public final int     inclusion;

    /**
     * Constructor of boolean key (boundary is inclusive)
     */
    public Key(boolean v) { 
        this(v, true);
    }

    /**
     * Constructor of byte key (boundary is inclusive)
     */
    public Key(byte v) { 
        this(v, true);
    }

    /**
     * Constructor of char key (boundary is inclusive)
     */
    public Key(char v) { 
        this(v, true);
    }

    /**
     * Constructor of short key (boundary is inclusive)
     */
    public Key(short v) { 
        this(v, true);
    }

    /**
     * Constructor of int key (boundary is inclusive)
     */
    public Key(int v) { 
        this(v, true);
    }

    /**
     * Constructor of long key (boundary is inclusive)
     */
    public Key(long v) { 
        this(v, true);
    }

    /**
     * Constructor of float key (boundary is inclusive)
     */
    public Key(float v) { 
        this(v, true);
    }

    /**
     * Constructor of double key (boundary is inclusive)
     */
    public Key(double v) { 
        this(v, true);
    }

    /**
     * Constructor of date key (boundary is inclusive)
     */
    public Key(java.util.Date v) { 
         this(v, true);
    }

    /**
     * Constructor of string key (boundary is inclusive)
     */
    public Key(String v) { 
        this(v, true);
    }

    /**
     * Constructor of array of char key (boundary is inclusive)
     */
    public Key(char[] v) { 
        this(v, true);
    }

    /**
     * Constructor of key with persistent object reference (boundary is inclusive)
     */
    public Key(IPersistent v) { 
        this(v, true);
    }

    private Key(int type, long lval, double dval, char[] sval, boolean inclusive) { 
        this.type = type;
        this.ival = (int)lval;
        this.lval = lval;
        this.dval = dval;
        this.sval = sval;
        this.inclusion = inclusive ? 1 : 0;
    }

    /**
     * Constructor of boolean key
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
     public Key(boolean v, boolean inclusive) { 
        this(ClassDescriptor.tpBoolean, v ? 1 : 0, 0.0, null, inclusive);
    }

    /**
     * Constructor of byte key
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(byte v, boolean inclusive) { 
        this(ClassDescriptor.tpByte, v, 0.0, null, inclusive);
    }

    /**
     * Constructor of char key
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(char v, boolean inclusive) { 
        this(ClassDescriptor.tpChar, v, 0.0, null, inclusive);
    }

    /**
     * Constructor of short key
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(short v, boolean inclusive) { 
        this(ClassDescriptor.tpShort, v, 0.0, null, inclusive);
    }

    /**
     * Constructor of int key
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(int v, boolean inclusive) { 
        this(ClassDescriptor.tpInt, v, 0.0, null, inclusive);
    }

    /**
     * Constructor of long key
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(long v, boolean inclusive) { 
        this(ClassDescriptor.tpLong, v, 0.0, null, inclusive);
    }

    /**
     * Constructor of float key
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(float v, boolean inclusive) { 
        this(ClassDescriptor.tpFloat, 0, v, null, inclusive);
    }

    /**
     * Constructor of double key
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(double v, boolean inclusive) { 
        this(ClassDescriptor.tpDouble, 0, v, null, inclusive);
    }

    /**
     * Constructor of date key
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(java.util.Date v, boolean inclusive) { 
        this(ClassDescriptor.tpDate, v.getTime(), 0.0, null, inclusive);
    }

    /**
     * Constructor of string key
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(String v, boolean inclusive) { 
        this(ClassDescriptor.tpString, 0, 0.0, v.toCharArray(), inclusive);
    }

    /**
     * Constructor of array of char key
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(char[] v, boolean inclusive) { 
        this(ClassDescriptor.tpString, 0, 0.0, v, inclusive);
    }

    /**
     * Constructor of key with persistent object reference
     * @param v key value
     * @param inclusive whether boundary is inclusive or exclusive
     */
    public Key(IPersistent v, boolean inclusive) { 
        this(ClassDescriptor.tpObject, v.getOid(), 0.0, null, inclusive);
    }
}


