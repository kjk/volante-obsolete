package org.garret.perst.impl;
import  org.garret.perst.*;
import  java.lang.ref.WeakReference;

public class WeakHashTable implements OidHashTable { 
    Entry table[];
    static final float loadFactor = 0.75f;
    int count;
    int threshold;

    public WeakHashTable(int initialCapacity) {
        threshold = (int)(initialCapacity * loadFactor);
        if (initialCapacity != 0) { 
            table = new Entry[initialCapacity];
        }
    }

    public synchronized boolean remove(int oid) {
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index], prev = null; e != null; prev = e, e = e.next) {
            if (e.oid == oid) {
                e.dirty = 0;
                e.ref.clear();
                count -= 1;
                if (prev != null) {
                    prev.next = e.next;
                } else {
                    tab[index] = e.next;
                }
                return true;
            }
        }
        return false;
    }

    public synchronized void put(int oid, IPersistent obj) { 
        WeakReference ref = new WeakReference(obj);
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.oid == oid) {
                e.ref = ref;
                return;
            }
        }
        if (count >= threshold) {
            // Rehash the table if the threshold is exceeded
            rehash();
            tab = table;
            index = (oid & 0x7FFFFFFF) % tab.length;
        } 

        // Creates the new entry.
        tab[index] = new Entry(oid, ref, tab[index]);
        count++;
    }
    
    public IPersistent get(int oid) {
        while (true) { 
            cs:synchronized(this) { 
                Entry tab[] = table;
                int index = (oid & 0x7FFFFFFF) % tab.length;
                for (Entry e = tab[index] ; e != null ; e = e.next) {
                    if (e.oid == oid) {
                        IPersistent obj = (IPersistent)e.ref.get();
                        if (obj == null && e.dirty != 0) { 
                            break cs;
                        }
                        return obj;
                    }
                }
                return null;
            }
            System.runFinalization();
        } 
    }
    
    public void flush() {
        while (true) { 
            cs:synchronized(this) { 
                for (int i = 0; i < table.length; i++) { 
                    for (Entry e = table[i]; e != null; e = e.next) { 
                        IPersistent obj = (IPersistent)e.ref.get();
                        if (obj != null) { 
                            if (obj.isModified()) { 
                                obj.store();
                            }
                        } else if (e.dirty != 0) { 
                            break cs;
                        }
                    }
                }
                return;
            }
            System.runFinalization();
        }
    }

    public void invalidate() {
        while (true) { 
            cs:synchronized(this) { 
                for (int i = 0; i < table.length; i++) { 
                    for (Entry e = table[i]; e != null; e = e.next) { 
                        IPersistent obj = (IPersistent)e.ref.get();
                        if (obj != null) { 
                            if (obj.isModified()) { 
                                e.dirty = 0;
                                obj.invalidate();
                            }
                        } else if (e.dirty != 0) { 
                            break cs;
                        }
                    }
                }
                return;
            }
            System.runFinalization();
        }
    }

    void rehash() {
        int oldCapacity = table.length;
        Entry oldMap[] = table;
        int i;
        for (i = oldCapacity; --i >= 0;) {
            for (Entry prev = null, e = oldMap[i]; e != null; e = e.next) { 
                if (e.ref.get() == null && e.dirty == 0) { 
                    count -= 1;
                    if (prev == null) { 
                        oldMap[i] = e.next;
                    } else { 
                        prev.next = e.next;
                    }
                } else { 
                    prev = e;
                }
            }
        }
        
        if (count <= (threshold >>> 1)) {
            return;
        }
        int newCapacity = oldCapacity * 2 + 1;
        Entry newMap[] = new Entry[newCapacity];

        threshold = (int)(newCapacity * loadFactor);
        table = newMap;

        for (i = oldCapacity; --i >= 0 ;) {
            for (Entry old = oldMap[i]; old != null; ) {
                Entry e = old;
                old = old.next;

                int index = (e.oid & 0x7FFFFFFF) % newCapacity;
                e.next = newMap[index];
                newMap[index] = e;
            }
        }
    }

    public synchronized void setDirty(int oid) {
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index] ; e != null ; e = e.next) {
            if (e.oid == oid) {
                e.dirty += 1;
                return;
            }
        }
    }

    public synchronized void clearDirty(int oid) {
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index] ; e != null ; e = e.next) {
            if (e.oid == oid) {
                if (e.dirty > 0) { 
                    e.dirty -= 1;
                }
                return;
            }
        }
    }

    public int size() { 
        return count;
    }

    static class Entry { 
        Entry         next;
        WeakReference ref;
        int           oid;
        int           dirty;
        
        Entry(int oid, WeakReference ref, Entry chain) { 
            next = chain;
            this.oid = oid;
            this.ref = ref;
        }
    }
}







