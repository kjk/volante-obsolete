package org.nachodb;
import  org.nachodb.*;
import  java.util.*;

public abstract class PersistentCollection<T> extends PersistentResource implements Collection<T>
{
    public boolean containsAll(Collection<?> c) {
	Iterator<?> e = c.iterator();
	while (e.hasNext())
	    if(!contains(e.next()))
		return false;
	return true;
    }

    public boolean addAll(Collection<? extends T> c) {
	boolean modified = false;
	Iterator<? extends T> e = c.iterator();
	while (e.hasNext()) {
	    if (add(e.next()))
		modified = true;
	}
	return modified;
    }

    public boolean removeAll(Collection<?> c) {
        boolean modified = false;
        Iterator<?> i = c.iterator();
        while (i.hasNext()) {
            modified |= remove(i.next());
        }
        return modified;
    }

    public boolean retainAll(Collection<?> c) {
        ArrayList<T> toBeRemoved = new ArrayList<T>();
        Iterator<T> i = iterator();
        while (i.hasNext()) {
            T o = i.next();
            if (!c.contains(o)) {
                toBeRemoved.add(o);
            }
        }
        int n = toBeRemoved.size();
        for (int j = 0; j < n; j++) { 
            remove(toBeRemoved.get(j));
        }
        return n != 0;         
    }

    public boolean contains(Object o) {
	Iterator<T> e = iterator();
	if (o==null) {
	    while (e.hasNext())
		if (e.next()==null)
		    return true;
	} else {
	    while (e.hasNext())
		if (o.equals(e.next()))
		    return true;
	}
	return false;
    }

    public boolean remove(Object o) {
	Iterator<T> e = iterator();
	if (o==null) {
	    while (e.hasNext()) {
		if (e.next()==null) {
		    e.remove();
		    return true;
		}
	    }
	} else {
	    while (e.hasNext()) {
		if (o.equals(e.next())) {
		    e.remove();
		    return true;
		}
	    }
	}
	return false;
    }

    public boolean add(T o) {
	throw new UnsupportedOperationException();
    }

    public boolean isEmpty() {
	return size() == 0;
    }

    public PersistentCollection() {}

    public PersistentCollection(Storage storage) { 
        super(storage);
    }
}    
