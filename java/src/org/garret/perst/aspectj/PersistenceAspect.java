/*
 * Created on Jan 24, 2004
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package org.garret.perst.aspectj;

/**
 * @author Patrick Morris-Suzuki
 *
 */

import org.garret.perst.*;

privileged public aspect PersistenceAspect {
    declare parents: AutoPersist extends IPersistent;

    pointcut notPerstCode(): !within(org.garret.perst.*) && !within(org.garret.perst.impl.*) && !within(org.garret.perst.aspectj.*);
    
    pointcut persistentMethod(): 
        ((execution(!static * Persistent+.*(..)) && !execution(* Persistent.*(..)))
         || (execution(!static * AutoPersist+.*(..)) && !execution(* AutoPersist.*(..))))
        && !execution(void *.recursiveLoading());
                
    /*
     * Load object at the beginning of each instance mehtod of persistent capable object
     */         
    before(IPersistent t) : persistentMethod() && this(t) {
        t.load();
    }

    /*
     * Read access to fields of persistent object
     */ 
    before(StrictAutoPersist t): get(!transient !static * StrictAutoPersist+.*) && notPerstCode() && target(t)
    {
        t.load();
    }

    /*
     * Read access to fields of persistent object
     */ 
    before(StrictAutoPersist t): set(!transient !static * StrictAutoPersist+.*) && notPerstCode() && target(t) 
    {
        t.loadAndModify();
    }

    /*
     * Automatically notice modifications to any fields.
     */
    before(AutoPersist t):  set(!transient !static * AutoPersist+.*)
        && notPerstCode() && !withincode(*.new(..)) && target(t)  
    {
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
        if (storage != null && (state & RAW) != 0) { 
            storage.loadObject(this);
        }
    }
    
    public void AutoPersist.loadAndModify() {
        load();
        modify();
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
