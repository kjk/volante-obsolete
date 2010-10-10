package org.nachodb.impl;
import  org.nachodb.*;

public class StrongHashTable implements OidHashTable { 
    Entry table[];
    static final float loadFactor = 0.75f;
    int count;
    int threshold;

    public StrongHashTable(int initialCapacity) {
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
                e.obj = null;
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
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index]; e != null; e = e.next) {
            if (e.oid == oid) {
                e.obj = obj;
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
        tab[index] = new Entry(oid, obj, tab[index]);
        count++;
    }

    public synchronized IPersistent get(int oid) {
        Entry tab[] = table;
        int index = (oid & 0x7FFFFFFF) % tab.length;
        for (Entry e = tab[index] ; e != null ; e = e.next) {
            if (e.oid == oid) {
                return e.obj;
            }
        }
        return null;
    }
    
    void rehash() {
        int oldCapacity = table.length;
        Entry oldMap[] = table;
        int i;

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

    public synchronized void flush() {
        for (int i = 0; i < table.length; i++) { 
            for (Entry e = table[i]; e != null; e = e.next) { 
                if (e.obj.isModified()) { 
                    e.obj.store();
                }
            }
        }
    }

    public synchronized void invalidate() {
        for (int i = 0; i < table.length; i++) { 
            for (Entry e = table[i]; e != null; e = e.next) { 
                if (e.obj.isModified()) { 
                    e.obj.invalidate();
                }
            }
            table[i] = null;
        }
        count = 0;
    }

    public void setDirty(int oid) {
    } 

    public void clearDirty(int oid) {
    }

    public int size() { 
        return count;
    }

    static class Entry { 
        Entry         next;
        IPersistent   obj;
        int           oid;
        
        Entry(int oid, IPersistent obj, Entry chain) { 
            next = chain;
            this.oid = oid;
            this.obj = obj;
        }
    }
}







