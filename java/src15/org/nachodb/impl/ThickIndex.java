package org.nachodb.impl;
import  org.nachodb.*;

import java.util.*;

class ThickIndex<T extends IPersistent> extends PersistentCollection<T> implements Index<T> { 
    private Index<IPersistent> index;
    private int                nElems;

    static final int BTREE_THRESHOLD = 128;

    ThickIndex(Class keyType, StorageImpl db) { 
        super(db);
        index = db.<IPersistent>createIndex(keyType, true);
    }
    
    ThickIndex() {}

    public T get(Key key) {
        IPersistent s = index.get(key);
        if (s == null) { 
            return null;
        }
        if (s instanceof Relation) { 
            Relation r = (Relation)s;
            if (r.size() == 1) { 
                return (T)r.get(0);
            }
        }
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }
                  
    public T get(Object key) {
        return get(Btree.getKeyFromObject(key));
    }

    public ArrayList<T> getList(Key from, Key till) { 
        return extendList(index.getList(from, till));
    }

    public ArrayList<T> getList(Object from, Object till) { 
        return extendList(index.getList(from, till));
    }
   
    public IPersistent[] get(Key from, Key till) {
        return extend(index.get(from, till));
    }
     
    public IPersistent[] get(Object from, Object till) {
        return extend(index.get(from, till));
    }
     
    private ArrayList<T> extendList(ArrayList s) { 
        ArrayList<T> list = new ArrayList<T>();
        for (int i = 0, n = s.size(); i < n; i++) { 
            list.addAll((Collection<T>)s.get(i));
        }
        return list;
    }


    private IPersistent[] extend( IPersistent[] s) { 
        ArrayList list = new ArrayList();
        for (int i = 0; i < s.length; i++) { 
            list.addAll((Collection)s[i]);
        }
        return (IPersistent[])list.toArray(new IPersistent[list.size()]);
    }

    public T get(String key) {
        return get(new Key(key));
    }
                      
    public IPersistent[] getPrefix(String prefix) { 
        return extend(index.getPrefix(prefix));
    }
    
    public ArrayList<T> getPrefixList(String prefix) { 
        return extendList(index.getPrefixList(prefix));
    }
    
    public IPersistent[] prefixSearch(String word) { 
        return extend(index.prefixSearch(word));
    }
           
    public ArrayList<T> prefixSearchList(String word) { 
        return extendList(index.prefixSearchList(word));
    }
           
    public int size() { 
        return nElems;
    }
    
    public void clear() { 
        for (IPersistent p : index) { 
            p.deallocate();
        }
        index.clear();
        nElems = 0;
        modify();
    }

    public IPersistent[] toPersistentArray() { 
        return extend(index.toPersistentArray());
    }
        
    public Object[] toArray() {
        return toPersistentArray();
    }

    public <E> E[] toArray(E[] arr) { 
        ArrayList<E> list = new ArrayList<E>();
        for (IPersistent c : index) { 
            list.addAll((Collection<E>)c);
        }
        return list.toArray(arr);
    }

    static class ExtendIterator<E extends IPersistent> extends IterableIterator<E> {  
        public boolean hasNext() { 
            return inner != null;
        }

        public E next() { 
            E obj = inner.next();
            if (!inner.hasNext()) {                 
                if (outer.hasNext()) {
                    inner = ((Iterable<E>)outer.next()).iterator();
                } else { 
                    inner = null;
                }
            }
            return obj;
        }

        public void remove() { 
            throw new UnsupportedOperationException();
        }

        ExtendIterator(IterableIterator<?> iterable) { 
            this(iterable.iterator());
        }

        ExtendIterator(Iterator<?> iterator) { 
            outer = iterator;
            if (iterator.hasNext()) { 
                inner = ((Iterable<E>)iterator.next()).iterator();
            }
        }

        private Iterator    outer;
        private Iterator<E> inner;
    }

    static class ExtendEntry<E extends IPersistent> implements Map.Entry<Object,E> {
        public Object getKey() { 
            return key;
        }

        public E getValue() { 
            return value;
        }

        public E setValue(E value) { 
            throw new UnsupportedOperationException();
        }

        ExtendEntry(Object key, E value) {
            this.key = key;
            this.value = value;
        }

        private Object key;
        private E      value;
    }

    static class ExtendEntryIterator<E extends IPersistent> extends IterableIterator<Map.Entry<Object,E>> {  
        public boolean hasNext() { 
            return inner != null;
        }

        public Map.Entry<Object,E> next() { 
            ExtendEntry<E> curr = new ExtendEntry<E>(key, inner.next());
            if (!inner.hasNext()) {                 
                if (outer.hasNext()) {
                    Map.Entry entry = (Map.Entry)outer.next();
                    key = entry.getKey();
                    inner = ((Iterable<E>)entry.getValue()).iterator();
                } else { 
                    inner = null;
                }
            }
            return curr;
        }

        public void remove() { 
            throw new UnsupportedOperationException();
        }

        ExtendEntryIterator(IterableIterator<?> iterator) { 
            outer = iterator;
            if (iterator.hasNext()) { 
                Map.Entry entry = (Map.Entry)iterator.next();
                key = entry.getKey();
                inner = ((Iterable<E>)entry.getValue()).iterator();
            }
        }

        private Iterator    outer;
        private Iterator<E> inner;
        private Object      key;
    }


    public Iterator<T> iterator() { 
        return new ExtendIterator<T>(index.iterator());
    }
    
    public IterableIterator<Map.Entry<Object,T>> entryIterator() { 
        return new ExtendEntryIterator<T>(index.entryIterator());
    }

    public IterableIterator<T> iterator(Key from, Key till, int order) { 
        return new ExtendIterator<T>(index.iterator(from, till, order));
    }
        
    public IterableIterator<T> iterator(Object from, Object till, int order) { 
        return new ExtendIterator<T>(index.iterator(from, till, order));
    }
        
    public IterableIterator<Map.Entry<Object,T>> entryIterator(Key from, Key till, int order) { 
        return new ExtendEntryIterator<T>(index.entryIterator(from, till, order));
    }

    public IterableIterator<Map.Entry<Object,T>> entryIterator(Object from, Object till, int order) { 
        return new ExtendEntryIterator<T>(index.entryIterator(from, till, order));
    }

    public IterableIterator<T> prefixIterator(String prefix) { 
        return new ExtendIterator<T>(index.prefixIterator(prefix));
    }

    public Class getKeyType() { 
        return index.getKeyType();
    }

    public boolean put(Key key, T obj) { 
        IPersistent s = index.get(key);
        if (s == null) { 
            Relation<T,ThickIndex> r = getStorage().<T,ThickIndex>createRelation(null);
            r.add(obj);
            index.put(key, r);
        } else if (s instanceof Relation) { 
            Relation r = (Relation)s;
            if (r.size() == BTREE_THRESHOLD) {
                IPersistentSet<T> ps = getStorage().<T>createSet();
                for (int i = 0; i < BTREE_THRESHOLD; i++) { 
                    ps.add((T)r.get(i));
                }
                ps.add(obj);
                index.set(key, ps);
                r.deallocate();
            } else { 
                r.add(obj);
            }
        } else { 
            ((IPersistentSet<T>)s).add(obj);
        }
        nElems += 1;
        modify();
        return true;
    }

    public T set(Key key, T obj) {
        IPersistent s = index.get(key);
        if (s == null) { 
            Relation<T,ThickIndex> r = getStorage().<T,ThickIndex>createRelation(null);
            r.add(obj);
            index.put(key, r);
            nElems += 1;
            modify();
            return null;
        } else if (s instanceof Relation) { 
            Relation r = (Relation)s;
            if (r.size() == 1) {
                IPersistent prev = r.get(0);
                r.set(0, obj);
                return (T)prev;
            } 
        }
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    public void remove(Key key, T obj) { 
        IPersistent s = index.get(key);
        if (s instanceof Relation) { 
            Relation r = (Relation)s;
            int i = r.indexOf(obj);
            if (i >= 0) { 
                r.remove(i);
                if (r.size() == 0) { 
                    index.remove(key, r);
                    r.deallocate();
                }
                nElems -= 1;
                modify();
                return;
            }
        } else if (s instanceof IPersistentSet) { 
            IPersistentSet ps = (IPersistentSet)s;
            if (ps.remove(obj)) { 
                if (ps.size() == 0) { 
                    index.remove(key, ps);
                    ps.deallocate();
                }                    
                nElems -= 1;
                modify();
                return;
            }
        }
        throw new StorageError(StorageError.KEY_NOT_FOUND);
    }

    public T remove(Key key) {
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    public boolean put(Object key, T obj) {
        return put(Btree.getKeyFromObject(key), obj);
    }

    public T set(Object key, T obj) {
        return set(Btree.getKeyFromObject(key), obj);
    }

    public void remove(Object key, T obj) {
        remove(Btree.getKeyFromObject(key), obj);
    }

    public T remove(String key) {
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    public T removeKey(Object key) {
        throw new StorageError(StorageError.KEY_NOT_UNIQUE);
    }

    public void deallocate() {
        clear();
        index.deallocate();
        super.deallocate();
    }
}