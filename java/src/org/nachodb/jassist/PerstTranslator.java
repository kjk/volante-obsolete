package org.nachodb.jassist;

import javassist.*;
import javassist.expr.*;

/**
 * This class is designed to be used with 
 * <A href="http://www.csg.is.titech.ac.jp/~chiba/javassist/">JAssist</A> to provide transparent 
 * persistence  by preprocessing protect class files. This translator automatically
 * made persistent capable all class matching specifying name patterns. With this 
 * translator it is not required for application classes to be derived from Persistent class
 * and provide empty default constructor. 
 * Example of usage:
 * <pre>
 * package com.mycompany.mypackage;
 * import org.nachodb.jassist.PerstTranslator;
 * import javassist.*;
 * public class MyApp { 
 *     public static void main(String[] args) { 
 *         Translatator trans = new PerstTranslator(new String[]{"com.mycompany.*"});
 *         ClassPool pool = ClassPool.getDefault(trans);
 *         Loader cl = new Loader(pool);
 *         cl.run("Main", args);
 *     }
 * }
 * </pre>
 * In this example all classes from <code>com.mycompany.mypackage</code> except 
 * MyApp will be loaded by JAssist class loader and automatically made persistent capable.
 */
public class PerstTranslator implements Translator { 
    protected boolean isPersistent(String className) { 
        for (int i = 0; i < classNamePatterns.length; i++) { 
            String pattern = classNamePatterns[i];
            if (className.equals(pattern) 
                || (pattern.endsWith("*") 
                    && className.startsWith(pattern.substring(0, pattern.length()-1))
                    && !className.endsWith("LoadFactory")
                    && !className.startsWith("org.nachodb")))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Create Perst translator which made all classes persistent capable 
     * (excluding <code>java.*</code> and <code>org.nachodb.*</code>
     * packages)
     */
    public PerstTranslator() { 
        this(new String[]{"*"});
    }

    /**
     * Create Perst translator with specified list of class name patterns.
     * Classes which fully qualified name matchs one of the patterns are made persistent capable.
     */
    public PerstTranslator(String[] classNamePatterns) { 
        this.classNamePatterns = classNamePatterns;
    }

    public void start(ClassPool pool) throws NotFoundException { 
        persistent = pool.get("org.nachodb.Persistent");
        persistentInterface = pool.get("org.nachodb.IPersistent");
        factory = pool.get("org.nachodb.impl.LoadFactory");
        object = pool.get("java.lang.Object");
        isRecursive = persistent.getDeclaredMethod("recursiveLoading"); 
        constructorParams = new CtClass[]{pool.get("org.nachodb.impl.ClassDescriptor")};
        serializable = pool.get("org.nachodb.impl.FastSerializable");
        pack = serializable.getDeclaredMethod("pack");
        unpack = serializable.getDeclaredMethod("unpack");
        create = factory.getDeclaredMethod("create");
    }

    private boolean addSerializeMethods(CtClass cc, boolean callSuper)
        throws NotFoundException, CannotCompileException
    {        
        CtField[] fields = cc.getDeclaredFields();
        int size = 0;
        StringBuffer sb = new StringBuffer();
        sb.append(callSuper ? "{$2=super.pack($1, $2);$1.extend($2" : "{$1.extend($2");
        for (int i = 0; i < fields.length; i++) { 
            CtField f = fields[i];
            if ((f.getModifiers() & (Modifier.STATIC|Modifier.TRANSIENT)) == 0) { 
                CtClass type = f.getType();
                if (type.isPrimitive()) { 
                    if (type == CtClass.booleanType || type == CtClass.byteType) { 
                        size += 1; 
                    } else if (type == CtClass.charType || type == CtClass.shortType) { 
                        size += 2;
                    } else if (type == CtClass.longType || type == CtClass.doubleType) { 
                        size += 8;
                    } else { 
                        size += 4;
                    }
                } else if (type.getName().equals("java.lang.String")) {
                    sb.append("+org.nachodb.impl.Bytes#sizeof(");
                    sb.append(f.getName());
                    sb.append(",$3)");
                } else { 
                    return false;
                }
            }
        }
        cc.addInterface(serializable);
        
        CtMethod m = new CtMethod(pack, cc, null);
        sb.append('+');
        sb.append(size);
        sb.append(");");
        for (int i = 0; i < fields.length; i++) { 
            CtField f = fields[i];
            if ((f.getModifiers() & (Modifier.STATIC|Modifier.TRANSIENT)) == 0) { 
                CtClass type = f.getType();
                String name = f.getName();
                if (type == CtClass.booleanType) { 
                    sb.append("$1.arr[$2++]=(byte)(");
                    sb.append(name);
                    sb.append("?1:0);");
                } else if (type == CtClass.charType) { 
                    sb.append("org.nachodb.impl.Bytes#pack2($1.arr,$2,(short)");
                    sb.append(name);
                    sb.append(");$2+=2;");
                } else if (type == CtClass.byteType) { 
                    sb.append("$1.arr[$2++]=");
                    sb.append(name);
                    sb.append(";");
                } else if (type == CtClass.shortType) { 
                    sb.append("org.nachodb.impl.Bytes#pack2($1.arr,$2,");
                    sb.append(name);
                    sb.append(");$2+=2;");
                } else if (type == CtClass.intType) { 
                    sb.append("org.nachodb.impl.Bytes#pack4($1.arr,$2,");
                    sb.append(name);
                    sb.append(");$2+=4;");
                } else if (type == CtClass.longType) { 
                    sb.append("org.nachodb.impl.Bytes#pack8($1.arr,$2,");
                    sb.append(name);
                    sb.append(");$2+=8;");
                } else if (type == CtClass.doubleType) { 
                    sb.append("org.nachodb.impl.Bytes#packF8($1.arr,$2,");
                    sb.append(name);
                    sb.append(");$2+=8;");
                } else if (type == CtClass.floatType) { 
                    sb.append("org.nachodb.impl.Bytes#packF4($1.arr,$2,");
                    sb.append(name);
                    sb.append(");$2+=4;");
               } else { 
                    sb.append("$2=org.nachodb.impl.Bytes#packStr($1.arr,$2,");
                    sb.append(name);
                    sb.append(",$3);");
                }
            }
        }
        sb.append("return $2;}");
        m.setBody(sb.toString());
        cc.addMethod(m);

        m = new CtMethod(unpack, cc, null);
        sb = new StringBuffer();
        sb.append(callSuper ? "{$2=super.unpack($1, $2);" : "{");
        for (int i = 0; i < fields.length; i++) { 
            CtField f = fields[i];
            if ((f.getModifiers() & (Modifier.STATIC|Modifier.TRANSIENT)) == 0) { 
                CtClass type = f.getType();
                String name = f.getName();
                sb.append(name);
                sb.append('=');
                if (type == CtClass.booleanType) { 
                    sb.append("$1[$2++]!=0;");
                } else if (type == CtClass.charType) { 
                    sb.append("(char)org.nachodb.impl.Bytes#unpack2($1,$2);$2+=2;");
                } else if (type == CtClass.byteType) { 
                    sb.append("$1[$2++];");
                } else if (type == CtClass.shortType) { 
                    sb.append("org.nachodb.impl.Bytes#unpack2($1,$2);$2+=2;");
                } else if (type == CtClass.intType) { 
                    sb.append("org.nachodb.impl.Bytes#unpack4($1,$2);$2+=4;");
                } else if (type == CtClass.longType) { 
                    sb.append("org.nachodb.impl.Bytes#unpack8($1,$2);$2+=8;");
                } else if (type == CtClass.doubleType) { 
                    sb.append("org.nachodb.impl.Bytes#unpackF8($1,$2);$2+=8;");
                } else if (type == CtClass.floatType) { 
                    sb.append("org.nachodb.impl.Bytes#unpackF4($1,$2);$2+=4;");
               } else { 
                    sb.append("org.nachodb.impl.Bytes#unpackStr($1,$2,$3);$2+=org.nachodb.impl.Bytes#sizeof($1,$2);");
                }
            }
        }
        sb.append("return $2;}");
        m.setBody(sb.toString());
        cc.addMethod(m);
        return true;
    }

    private void preprocessMethods(CtClass cc, boolean insertLoad, boolean wrapFieldAccess)  throws CannotCompileException 
    {
        CtMethod[] methods = cc.getDeclaredMethods();
        for (int i = 0; i < methods.length; i++) { 
            CtMethod m = methods[i];            
            if (wrapFieldAccess) { 
                m.instrument(new ExprEditor() { 
                    public void edit(FieldAccess fa) throws CannotCompileException { 
                        try { 
                            if ((fa.getField().getModifiers() & (Modifier.TRANSIENT|Modifier.STATIC)) == 0
                                && fa.getField().getDeclaringClass().subtypeOf(persistentInterface))
                            {
                                if (fa.isWriter()) { 
                                    fa.replace("{ $0.loadAndModify(); $proceed($$); }");
                                }
                                // isSelfReader is my extension of JAssist, if you 
                                // use original version of JAssist comment the 
                                // branch below or replace "else if" with "else".
                                // In first case Perst will not be able to handle
                                // access to foreign (non-this) fields. You should use
                                // getter/setter methods instead. 
                                // In second case access to foreign fields still will be possible,
                                // but with significant degradation of performance and 
                                // increased code size, because in this case before ALL access
                                // to fields of persistent capable object call of load() method
                                // will be inserted.
                                else if (!fa.isSelfReader()) 
                                { 
                                    fa.replace("{ $0.load(); $_ = $proceed($$); }");
                                }
                            }
                        } catch (NotFoundException x) {}
                    }
                });
            }
            if (insertLoad 
                && !"recursiveLoading".equals(m.getName())
                && (m.getModifiers() & (Modifier.STATIC|Modifier.ABSTRACT)) == 0)  
            { 
                m.insertBefore("load();");
            }
        }
    }


    public void onLoad(ClassPool pool, String className)
        throws NotFoundException, CannotCompileException
    {
        onWrite(pool, className);
    }

    public void onWrite(ClassPool pool, String className)
        throws NotFoundException, CannotCompileException 
    {
        CtClass cc = pool.get(className);
        try {
            if (isPersistent(className)) {                
                CtClass base = cc.getSuperclass();
                CtConstructor cons = new CtConstructor(constructorParams, cc);            
                if (base.subclassOf(persistent) || base == object) { 
                    cons.setBody(null);
                    cc.addConstructor(cons);
                    if (base == object) { 
                        cc.setSuperclass(persistent);                    
                    }
                } else { 
                    if (!isPersistent(base.getName())) { 
                        throw new NotFoundException("Base class " + base.getName()
                                                    + " was not declared as persistent");
                    } 
                    cons.setBody("super($0);");
                    cc.addConstructor(cons);
                }
                preprocessMethods(cc, true, true);
                if (base == persistent || base == object) { 
                    CtMethod m = new CtMethod(isRecursive, cc, null);
                    m.setBody("return false;");
                    cc.addMethod(m);
                    addSerializeMethods(cc, false);
                } else if (base.subtypeOf(serializable)) { 
                    addSerializeMethods(cc, true);
                }
                if ((cc.getModifiers() & Modifier.PRIVATE) == 0) { 
                    CtClass f = pool.makeClass(className + "LoadFactory");
                    f.addInterface(factory);
                    CtMethod c = new CtMethod(create, f, null);
                    c.setBody("return new " + className + "($1);");
                    f.addMethod(c);
                    CtNewConstructor.defaultConstructor(f);
                }
            } else { 
                preprocessMethods(cc, 
                                  cc.subtypeOf(persistent) && cc != persistent, 
                                  !className.startsWith("org.nachodb")); 
            }
        } catch(Exception x) { x.printStackTrace(); }
    }

    CtClass       persistent;
    CtClass       persistentInterface;
    CtClass       object;
    CtClass       factory;
    CtClass[]     constructorParams;
    CtMethod      create;
    CtMethod      isRecursive;
    String[]      classNamePatterns;
    CtClass       serializable;
    CtMethod      pack;
    CtMethod      unpack;
}
            

