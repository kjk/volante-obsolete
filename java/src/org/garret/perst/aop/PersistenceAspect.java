/*
 * Created on Jan 24, 2004
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package org.garret.perst.aop;

/**
 * @author Patrick Morris-Suzuki
 *
 */

import org.garret.perst.*;

privileged public aspect PersistenceAspect {
    declare parents: AutoPersist extends IPersistent;

    pointcut notPerstCode(): !within(org.garret.perst.*) && !within(org.garret.perst.impl.*) && !within(org.garret.perst.aop.*);
    
    pointcut persistentMethod(): execution(!static * AutoPersist+.*(..)) && notPerstCode();
                
    /*
     * Load object at the beginning of each instance mehtod of persistent capable object
     */         
    before(AutoPersist t) : persistentMethod() && this(t) {
        t.load();
    }

    /*
     * Uncomment this code if you want to allow access to field of other objects
     * (non-this access). It will significantly decrease efficiency of result code,
     * because any access to instance fiels (doesn't matter access to self or foreign
     * field) will be prepended by invocation of load() method. As far as 
     * OOP design rules recommend to made all fields private or protected
     * and access them only through methods, I highly recommend you to avoid 
     * access to foreign fields and do not use this code. Access fields through getter
     * methods will be in any case much efficient.
     * 
    pointcut persistentFieldAccess(): (set(!transient !static * AutoPersist+.*) 
        || get(!transient !static * AutoPersist+.*)) && notPerstCode();

    before(AutoPersist t) : persitentFieldAccess() && targer(t) {
        t.load();
    }
    */

    pointcut fieldSet(): set(!transient !static * AutoPersist+.*)
        && notPerstCode() && !withincode(*.new(..));
    
    /*
     * Automatically notice modifications to any fields.
     */
    before(AutoPersist t):  fieldSet() && target(t)  {
        t.modify();
    }
    
    public void AutoPersist.assignOid(Storage s, int o, boolean r) {
        oid = o;
        storage = s;
        state = r? RAW : 0;
    }
    
    boolean around(AutoPersist me, Object other):
    execution(boolean AutoPersist+.equals(Object)) &&
        args(other) && target(me){
        if(other==null) return false;
        
        boolean isEqual;
        try{
            isEqual=proceed(me, other);
        } catch(ClassCastException excep){
                        if(!other.getClass().equals(me.getClass()))
                            return false;
                        else
                            throw excep;
        }
        if(!isEqual){
            if(other!=null && other instanceof IPersistent){
                if(((IPersistent)other).getOid()==me.oid) isEqual=true;
            }
        }
        return isEqual;
    }
    
    int around(AutoPersist me):
    execution(int AutoPersist+.hashCode()) && target(me){
        return me.oid;
    }
    
    public void AutoPersist.commit() {
        if (storage !=null)
            storage.commit();
    }
    
    public void AutoPersist.load() {
        if (storage != null) { 
            storage.loadObject(this);
        }
    }
    
    public final boolean AutoPersist.isRaw() { 
        return (state & RAW) != 0;
    } 
    
    public final boolean AutoPersist.isModified() { 
        return (state & DIRTY) != 0;
    } 
    
    public final boolean AutoPersist.isPersistent() { 
        return oid != 0;
    }
    
    public void AutoPersist.makePersistent(Storage storage) { 
        if (oid == 0) { 
            storage.storeObject(this);
        }
    }

    public void AutoPersist.store() {
        if ((state & RAW) != 0) { 
            throw new StorageError(StorageError.ACCESS_TO_STUB);
        }
        if (storage != null) { 
            storage.storeObject(this);
            state &= ~DIRTY;
        }
    }
    
    public void AutoPersist.modify() { 
        if ((state & DIRTY) == 0 && storage != null) { 
            if ((state & RAW) != 0) { 
                throw new StorageError(StorageError.ACCESS_TO_STUB);
            }
            storage.modifyObject(this);
            state |= DIRTY;
        }
    }
    
    public final int AutoPersist.getOid() {
        return oid;
    }
    
    public void AutoPersist.deallocate() { 
        if (storage != null) { 
            storage.deallocateObject(this);
            state = 0;
            storage = null;
        }
    }
    
    public boolean AutoPersist.recursiveLoading() {
        return false;
    }
    
    public final Storage AutoPersist.getStorage() {
        return storage;
    }
    
    public void AutoPersist.onLoad() {
    }
    
    public void AutoPersist.invalidate() { 
        state |= RAW;
    }
    
    public void AutoPersist.finalize() { 
        if ((state & DIRTY) != 0 && storage != null) { 
            storage.storeFinalizedObject(this);
            state &= ~DIRTY;
        }
    }
    
    private transient Storage AutoPersist.storage;
    private transient int     AutoPersist.oid;
    private transient int     AutoPersist.state;
    
    private static final int RAW   = 1;
    private static final int DIRTY = 2;
}
