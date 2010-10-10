package org.nachodb.impl;
import  org.nachodb.*;
import  java.util.*;
import  java.lang.reflect.Array;

public class LinkImpl<T extends IPersistent> implements Link<T> { 
    private final void modify() { 
        if (owner != null) { 
            owner.modify();
        }
    }

    public int size() {
        return used;
    }

    public void setSize(int newSize) { 
        if (newSize < used) { 
            for (int i = used; --i >= newSize; arr[i] = null);
        } else { 
            reserveSpace(newSize - used);            
        }
        used = newSize;
    }

    public T get(int i) {
        if (i < 0 || i >= used) { 
            throw new IndexOutOfBoundsException();
        }
        return (T)loadElem(i);
    }

    public IPersistent getRaw(int i) {
        if (i < 0 || i >= used) { 
            throw new IndexOutOfBoundsException();
        }
        return arr[i];
    }

    public void pin() { 
        for (int i = 0, n = used; i < n; i++) { 
            arr[i] = loadElem(i);
        }
    }

    public void unpin() { 
        for (int i = 0, n = used; i < n; i++) { 
            IPersistent elem = arr[i];
            if (elem != null && !elem.isRaw() && elem.isPersistent()) { 
                arr[i] = new PersistentStub(elem.getStorage(), elem.getOid());
            }
        }
    }


    public void set(int i, T obj) {
        if (i < 0 || i >= used) { 
            throw new IndexOutOfBoundsException();
        }
        arr[i] = obj;
        modify();
    }

    public boolean isEmpty() {
        return used == 0;
    }

    public void remove(int i) {
        if (i < 0 || i >= used) { 
            throw new IndexOutOfBoundsException();
        }
        used -= 1;
        System.arraycopy(arr, i+1, arr, i, used-i);
        arr[used] = null;
        modify();
    }

    void reserveSpace(int len) { 
        if (used + len > arr.length) { 
            IPersistent[] newArr = new IPersistent[used + len > arr.length*2 ? used + len : arr.length*2];
            System.arraycopy(arr, 0, newArr, 0, used);
            arr = newArr;
        }
        modify();
    }

    public void insert(int i, T obj) { 
         if (i < 0 || i > used) { 
            throw new IndexOutOfBoundsException();
        }
        reserveSpace(1);
        System.arraycopy(arr, i, arr, i+1, used-i);
        arr[i] = obj;
        used += 1;
    }

    public boolean add(T obj) {
        reserveSpace(1);
        arr[used++] = obj;
        return true;
    }

    public void addAll(T[] a) {
        addAll(a, 0, a.length);
    }
    
    public void addAll(T[] a, int from, int length) {
        reserveSpace(length);
        System.arraycopy(a, from, arr, used, length);
        used += length;
    }

    public boolean addAll(Link<T> link) {        
        int n = link.size();
        reserveSpace(n);
        for (int i = 0, j = used; i < n; i++, j++) { 
            arr[j] = link.getRaw(i);
        }
        used += n;
        return true;
    }

    public Object[] toArray() {
        return toPersistentArray();
    }

    public IPersistent[] toRawArray() {
        return arr;
    }

    public IPersistent[] toPersistentArray() {
        IPersistent[] a = new IPersistent[used];
        for (int i = used; --i >= 0;) { 
            a[i] = loadElem(i);
        }
        return a;
    }
    
    public <T> T[] toArray(T[] arr) {
        if (arr.length < used) { 
            arr = (T[])Array.newInstance(arr.getClass().getComponentType(), used);
        }
        for (int i = used; --i >= 0;) { 
            arr[i] = (T)loadElem(i);
        }
        if (arr.length > used) { 
            arr[used] = null;
        }
        return arr;
    }
    
    public boolean contains(Object obj) {
        return indexOf(obj) >= 0;
    }

    public int indexOf(Object obj) {
        int oid;
        if (obj instanceof IPersistent && (oid = ((IPersistent)obj).getOid()) != 0) { 
            for (int i = used; --i >= 0;) {
                IPersistent elem = arr[i];
                if (elem != null && elem.getOid() == oid) {
                    return i;
                }
            }
        } else { 
            for (int i = used; --i >= 0;) {
                if (arr[i] == obj) {
                    return i;
                }
            }
        }
        return -1;
    }
    
    public boolean containsElement(int i, T obj) {
        IPersistent elem = arr[i];
        return elem == obj || (elem != null && elem.getOid() != 0 && elem.getOid() == obj.getOid());
    }

    public void clear() { 
        for (int i = used; --i >= 0;) { 
            arr[i] = null;
        }
        used = 0;
        modify();
    }

    static class LinkIterator<T extends IPersistent> implements Iterator<T> { 
        private Link<T> link;
        private int     i;

        LinkIterator(Link<T> link) { 
            this.link = link;
        }

        public boolean hasNext() {
            return i < link.size();
        }

        public T next() throws NoSuchElementException { 
            if (!hasNext()) { 
                throw new NoSuchElementException();
            }
            return link.get(i++);
        }

        public void remove() {
            link.remove(i);
        }
    }

    public boolean remove(Object o) {
        int i = indexOf(o);
        if (i >= 0) { 
            remove(i);
            return true;
        }
        return false;
    }
        
    public boolean containsAll(Collection<?> c) {
	Iterator<?> e = c.iterator();
	while (e.hasNext()) {
	    if(!contains(e.next())) {
		return false;
            }
        }
	return true;
    }

    public boolean addAll(Collection<? extends T> c) {
	boolean modified = false;
	Iterator<? extends T> e = c.iterator();
	while (e.hasNext()) {
	    if (add(e.next())) { 
		modified = true;
            }
	}
	return modified;
    }

    public boolean removeAll(Collection<?> c) {
	boolean modified = false;
	Iterator<?> e = iterator();
	while (e.hasNext()) {
	    if (c.contains(e.next())) {
		e.remove();
		modified = true;
	    }
	}
	return modified;
    }

    public boolean retainAll(Collection<?> c) {
	boolean modified = false;
	Iterator<T> e = iterator();
	while (e.hasNext()) {
	    if (!c.contains(e.next())) {
		e.remove();
		modified = true;
	    }
	}
	return modified;
    }

    public Iterator<T> iterator() { 
        return new LinkIterator<T>(this);
    }

    private final T loadElem(int i) 
    {
        IPersistent elem = arr[i];
        if (elem != null && elem.isRaw()) { 
            elem = ((StorageImpl)elem.getStorage()).lookupObject(elem.getOid(), null);
        }
        return (T)elem;
    }

    LinkImpl() {}

    LinkImpl(int initSize) {
        arr = new IPersistent[initSize];
    }

    LinkImpl(T[] arr, IPersistent owner) { 
        this.arr = arr;
        this.owner = owner;
        used = arr.length;
    }

    IPersistent[] arr;
    int           used;
    transient IPersistent owner;
}
