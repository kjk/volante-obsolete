package org.garret.perst;

import java.util.Iterator;
import java.util.Map;
import java.lang.reflect.Field;

/**
 * Interface of indexed field.
 * Index is used to provide fast access to the object by the value of indexed field. 
 * Objects in the index are stored ordered by the value of indexed field. 
 * It is possible to select object using exact value of the key or 
 * select set of objects which key belongs to the specified interval 
 * (each boundary can be specified or unspecified and can be inclusive or exclusive)
 * Key should be of scalar, String, java.util.Date or peristent object type.
 */
public interface FieldIndex<T extends IPersistent> extends IPersistent, Iterable<T> { 
    /**
     * Get object by key (exact match)     
     * @param key specified key. It should match with type of the index and should be inclusive.
     * @return object with this value of the key or <code>null</code> if key nmot found
     * @exception StorageError(StorageError.KEY_NOT_UNIQUE) exception if there are more than 
     * one objects in the index with specified value of the key.
     */
    public T get(Key key);
    
    /**
     * Get object by string key (exact match)     
     * @param key string key 
     * @return object with this value of the key or <code>null</code> if key not[ found
     * @exception StorageError(StorageError.KEY_NOT_UNIQUE) exception if there are more than 
     * one objects in the index with specified value of the key.
     */
    public T get(String key);
    
    /**
     * Get objects with string key prefix 
     * @param prefix string key prefix
     * @return array of objects which key starts with this prefix 
     */
    public T[] getPrefix(String prefix);
    
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
    public T[] get(Key from, Key till);

    /**
     * Locate all objects which key is prefix of specified word.
     * @param word string which prefixes are located in index
     * @return array of objects which key is prefix of specified word, ordered by key value
     */
    public T[] prefixSearch(String word);
    
    /**
     * Put new object in the index. 
     * @param obj object to be inserted in index. Object should contain indexed field. 
     * Object can be not yet peristent, in this case
     * its forced to become persistent by assigning OID to it.
     * @return <code>true</code> if object is successfully inserted in the index, 
     * <code>false</code> if index was declared as unique and there is already object with such value
     * of the key in the index. 
     */
    public boolean put(T obj);

    /**
     * Associate new object with the key specified by object field value. 
     * If there is already object with such key in the index, 
     * then it will be removed from the index and new value associated with this key.
     * @param obj object to be inserted in index. Object should contain indexed field. 
     * Object can be not yet peristent, in this case
     * its forced to become persistent by assigning OID to it.
     * @return object previously associated with this key, <code>null</code> if there was no such object
     */
    public T set(T obj);

    /**
     * Assign to the integer indexed field unique autoicremented value and 
     * insert object in the index. 
     * @param obj object to be inserted in index. Object should contain indexed field
     * of integer (<code>int</code> or <code>long</code>) type.
     * This field is assigned unique value (which will not be reused while 
     * this index exists) and object is marked as modified.
     * Object can be not yet peristent, in this case
     * its forced to become persistent by assigning OID to it.
     * @exception StorageError(StorageError.INCOMPATIBLE_KEY_TYPE) when indexed field
     * has type other than <code>int</code> or <code>long</code>
     */
    public void append(T obj);

    /**
     * Remove object from the index
     * @param obj object removed from the index. Object should contain indexed field. 
     * @exception StorageError(StorageError.KEY_NOT_FOUND) exception if there is no such key in the index
     */
    public void remove(T obj);

    /**
     * Remove object with specified key from the unique index
     * @param key value of removed key
     * @return removed object
     * @exception StorageError(StorageError.KEY_NOT_FOUND) exception if there is no such key in the index,
     * or StorageError(StorageError.KEY_NOT_UNIQUE) if index is not unique.
     */
    public T remove(Key key);

    /**
     * Check if index contains specified object
     * @param obj object to be searched in the index. Object should contain indexed field. 
     * @return <code>true</code> if object is present in the index, <code>false</code> otherwise
     */
    public boolean contains(T obj);

    /**
     * Get number of objects in the index
     * @return number of objects in the index
     */
    public int size();
    
    /**
     * Remove all objects from the index
     */
    public void clear();

    /**
     * Get all objects in the index as array ordered by index key
     * @return array of specified type conatining objects in the index ordered by key value
     */
    public T[] toPersistentArray();

    /**
     * Get iterator for traversing all objects in the index. 
     * Objects are iterated in the ascent key order. 
     * You should not update/remove or add members to the index during iteration
     * @return index iterator
     */
    public Iterator<T> iterator();
    /**
     * Get iterator for traversing all entries in the index. 
     * Iterator next() method returns object implementing <code>Map.Entry</code> interface
     * which allows to get entry key and value.
     * Objects are iterated in the ascent key order. 
     * You should not update/remove or add members to the index during iteration
     * @return index iterator
     */
    public Iterator<Map.Entry<Object,T>> entryIterator();

    static final int ASCENT_ORDER  = 0;
    static final int DESCENT_ORDER = 1;
    /**
     * Get iterator for traversing objects in the index with key belonging to the specified range. 
     * You should not update/remove or add members to the index during iteration
     * @param from low boundary. If <code>null</code> then low boundary is not specified.
     * Low boundary can be inclusive or exclusive. 
     * @param till high boundary. If <code>null</code> then high boundary is not specified.
     * High boundary can be inclusive or exclusive. 
     * @param order <code>ASCENT_ORDER</code> or <code>DESCENT_ORDER</code>
     * @return selection iterator
     */
    public Iterator<T> iterator(Key from, Key till, int order);

    /**
     * Get iterator for traversing index entries with key belonging to the specified range. 
     * Iterator next() method returns object implementing <code>Map.Entry</code> interface
     * You should not update/remove or add members to the index during iteration
     * @param from low boundary. If <code>null</code> then low boundary is not specified.
     * Low boundary can be inclusive or exclusive. 
     * @param till high boundary. If <code>null</code> then high boundary is not specified.
     * High boundary can be inclusive or exclusive. 
     * @param order <code>ASCENT_ORDER</code> or <code>DESCENT_ORDER</code>
     * @return selection iterator
     */
    public Iterator<Map.Entry<Object,T>> entryIterator(Key from, Key till, int order);

    /**
     * Get iterator for records which keys started with specified prefix
     * Objects are iterated in the ascent key order. 
     * You should not update/remove or add members to the index during iteration
     * @param prefix key prefix
     * @return selection iterator
     */
    public Iterator<T> prefixIterator(String prefix);

    /**
     * Get class obejct objects which can be inserted in this index
     * @return class specified in Storage.createFielIndex method
     */
    public Class getIndexedClass();

    /**
     * Get fields used as a key
     * @return array of index key fields
     */
    public Field[] getKeyFields();
}

