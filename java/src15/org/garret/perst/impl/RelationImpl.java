package org.garret.perst.impl;
import  org.garret.perst.*;
import  java.util.Iterator;
import  java.util.Collection;

public class RelationImpl<M extends IPersistent, O extends IPersistent> extends Relation<M,O> {
    public int size() {
        return link.size();
    }
    
    public void setSize(int newSize) {
        link.setSize(newSize);
    }
    
    public boolean isEmpty() {
        return link.isEmpty();
    }
    
    public boolean remove(Object o) {
        if (link.remove(o)) { 
            modify();
            return true;
        }
        return false;
    }

    public M get(int i) {
        return link.get(i);
    }

    public IPersistent getRaw(int i) {
        return link.getRaw(i);
    }

    public void set(int i, M obj) {
        link.set(i, obj);
        modify();
    }

    public void remove(int i) {
        link.remove(i);
        modify();
    }

    public void insert(int i, M obj) {
        link.insert(i, obj);
        modify();
    }

    public boolean add(M obj) {
        modify();
        return link.add(obj);
    }

    public void addAll(M[] arr) {
        link.addAll(arr);
        modify();
    }

    public void addAll(M[] arr, int from, int length) {
        link.addAll(arr, from, length);
        modify();
    }

    public boolean addAll(Link<M> anotherLink) {
        if (link.addAll(anotherLink)) { 
            modify();
            return true;
        }
        return false;
    }

    public IPersistent[] toPersistentArray() {
        return link.toPersistentArray();
    }

    public IPersistent[] toRawArray() {
        return link.toRawArray();
    }

    public Object[] toArray() {
        return link.toArray();
    }

    public <T> T[] toArray(T[] arr) {
        return link.<T>toArray(arr);
    }

    public boolean contains(Object obj) {
        return link.contains(obj);
    }

    public int indexOf(Object obj) {
        return link.indexOf(obj);
    }
       
    public void clear() {
        link.clear();
        modify();
    }

    public Iterator<M> iterator() {
        return link.iterator();
    }
    

    public boolean containsAll(Collection<?> c) {        
        return link.containsAll(c);
    }

    public boolean containsElement(int i, M obj) {
        return link.containsElement(i, obj);
    }

    public boolean addAll(Collection<? extends M> c) {
        if (link.addAll(c)) { 
            modify();
            return true;
        }
        return false;
    }

    public boolean removeAll(Collection<?> c) {
        if (link.removeAll(c)) { 
            modify();
            return true;
        }
        return false;            
    }

    public boolean retainAll(Collection<?> c) {
        if (link.retainAll(c)) { 
            modify();
            return true;
        }
        return false;            
    }

    public void pin() { 
        link.pin();
    }
    
    public void unpin() { 
        link.unpin();
    }
    
    RelationImpl() {}

    RelationImpl(O owner) { 
        super(owner);
        link = new LinkImpl<M>(8);
    }

    Link<M> link;
}
