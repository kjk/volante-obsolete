package org.garret.perst.impl;
import  org.garret.perst.*;
import  java.lang.ref.WeakReference;

public class WeakHashTable { 
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

    public synchronized IPersistent get(int oid) {
	Entry tab[] = table;
	int index = (oid & 0x7FFFFFFF) % tab.length;
	for (Entry e = tab[index] ; e != null ; e = e.next) {
	    if (e.oid == oid) {
		return (IPersistent)e.ref.get();
	    }
	}
	return null;
    }

    void rehash() {
	int oldCapacity = table.length;
	Entry oldMap[] = table;
	int i;
	for (i = oldCapacity; --i >= 0;) {
	    for (Entry prev = null, e = oldMap[i]; e != null; e = e.next) { 
		if (e.ref.get() == null) { 
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

    public int size() { 
	return count;
    }
}

class Entry { 
    Entry         next;
    WeakReference ref;
    int           oid;

    Entry(int oid, WeakReference ref, Entry chain) { 
	next = chain;
	this.oid = oid;
	this.ref = ref;
    }
}







