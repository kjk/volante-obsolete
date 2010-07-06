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
        modify();
    }

    public void remove(int i) {
        link.remove(i);
        modify();
    }

    public void insert(int i, IPersistent obj) {
        link.insert(i, obj);
        modify();
    }

    public void add(IPersistent obj) {
        link.add(obj);
        modify();
    }

    public void addAll(IPersistent[] arr) {
        link.addAll(arr);
        modify();
    }

    public void addAll(IPersistent[] arr, int from, int length) {
        link.addAll(arr, from, length);
        modify();
    }

    public void addAll(Link anotherLink) {
        link.addAll(anotherLink);
        modify();
    }

    public IPersistent[] toArray() {
        return link.toArray();
    }

    public IPersistent[] toArray(IPersistent[] arr) {
        return link.toArray(arr);
    }

    public boolean contains(IPersistent obj) {
        return link.contains(obj);
    }

    public int indexOf(IPersistent obj) {
        return link.indexOf(obj);
    }
       
    public void clear() {
        link.clear();
        modify();
    }

    public Iterator iterator() {
        return link.iterator();
    }
    
    RelationImpl() {}

    RelationImpl(IPersistent owner) { 
        super(owner);
        link = new LinkImpl(8);
    }

    Link link;
}
