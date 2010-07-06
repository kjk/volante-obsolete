package org.garret.perst;

/**
 * Interface of object index.
 * Index is used to provide fast access to the object by key. 
 * Object in the index are stored ordered by key value. 
 * It is possible to select object using exact value of the key or 
 * select set of objects which key belongs to the specified interval 
 * (each boundary can be specified or unspecified and can be inclusive or exclusive)
 * Key should be of scalar, String, java.util.Date or peristent object type.
 */
public interface Index extends IPersistent, IResource { 
    /**
     * Get object by key (exact match)     
     * @param key specified key. It should match with type of the index and should be inclusive.
     * @return object with this value of the key or <code>null</code> if key nmot found
     * @exception StorageError(StorageError.KEY_NOT_UNIQUE) exception if there are more than 
     * one objects in the index with specified value of the key.
     */
    public IPersistent   get(Key key);
    
    /**
     * Get objects which key value belongs to the specified range.
     * Either from boundary, either till boundary either both of them can be <code>null</code>.
     * In last case the method returns all objects from the index.
     * @param from low boundary. If <code>null</code> then low boundary is not specified.
     * Low boundary can be inclusive or exclusive. 
     * @param till high boundary. If <code>null</code> then high boundary is not specified.
     * High boundary can be inclusive or exclusive. 
     * @return array of objects which keys belongs to the specified interval, ordered by key value
     */
    public IPersistent[] get(Key from, Key till);

    /**
     * Put new object in the index. 
     * @param key object key
     * @param obj object associated with this key. Object can be not yet peristent, in this case
     * its forced to become persistent by assigning OID to it.
     * @return <code>true</code> if object is successfully inserted in the index, 
     * <code>false</code> if index was declared as unique and there is already object with such value
     * of the key in the index. 
     */
    public boolean       put(Key key, IPersistent obj);

    /**
     * Associate new value with the key. If there is already object with such key in the index, 
     * then it will be removed from the index and new value associated with this key.
     * @param key object key
     * @param obj object associated with this key. Object can be not yet peristent, in this case
     * its forced to become persistent by assigning OID to it.
     */
    public void          set(Key key, IPersistent obj);

    /**
     * Remove object with specified key from the tree.
     * @param key value of the key of removed object
     * @param obj object removed from the index
     * @exception StorageError(StorageError.KEY_NOT_FOUND) exception if there is no such key in the index
     */
    public void          remove(Key key, IPersistent obj);

    /**
     * Remove key from the unique index.
     * @param key value of removed key
     * @exception StorageError(StorageError.KEY_NOT_FOUND) exception if there is no such key in the index,
     * or StorageError(StorageError.KEY_NOT_UNIQUE) if index is not unique.
     */
    public void          remove(Key key);

    /**
     * Get number of objects in the index
     * @return number of objects in the index
     */
    public int           size();
    
    /**
     * Remove all objects from the index
     */
    public void          clear();

    /**
     * Get all objects in the index as array orderd by index key
     * @return array of objects in the index ordered by key value
     */
    public IPersistent[] toArray();

    /**
     * Get iterator for traversing all objects in the index. 
     * Objects are iterated in the ascent key order. 
     * You should not update/remove or add members to the index during iteration
     * @return index iterator
     */
    public java.util.Iterator iterator();
}


