package org.garret.perst.impl;
import  org.garret.perst.*;
import  java.util.Iterator;

public class RelationImpl extends Relation {
    public int size() {
        return link.size();
    }
    
    public IPersistent get(int i) {
        return link.get(i);
    }

    public IPersistent getRaw(int i) {
        return link.getRaw(i);
    }

    public void set(int i, IPersistent obj) {
        link.set(i, obj);
    }

    public void remove(int i) {
        link.remove(i);
    }

    public void insert(int i, IPersistent obj) {
        link.insert(i, obj);
    }

    public void add(IPersistent obj) {
        link.add(obj);
    }

    public void addAll(IPersistent[] arr) {
        link.addAll(arr);
    }

    public void addAll(IPersistent[] arr, int from, int length) {
        link.addAll(arr, from, length);
    }

    public void addAll(Link anotherLink) {
        link.addAll(anotherLink);
    }

    public IPersistent[] toArray() {
        return link.toArray();
    }

    public boolean contains(IPersistent obj) {
        return link.contains(obj);
    }

    public int indexOf(IPersistent obj) {
        return link.indexOf(obj);
    }
       
    public void clear() {
        link.clear();
    }

    public Iterator iterator() {
        return link.iterator();
    }
    
    RelationImpl(IPersistent owner) { 
        super(owner);
        link = new LinkImpl(8);
    }

    LinkImpl link;
}
