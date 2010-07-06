package org.garret.perst;

/**
 * Interface of sorted collection.
 * Sorted collections keeps in members in order specified by comparator.
 * Members in the collections can be located using key or range of keys.
 * The SortedCollection is efficient container of objects for in-memory databases.
 * For databases which size is significatly larger than size of page pool, operation with SortedList
 * can cause trashing and so very bad performance. Unlike other index structures SortedCollection
 * doesn't store values of keys and so search in the collection requires fetching of its members.
 */
public interface SortedCollection<T extends IPersistent> extends IPersistent, IResource, Iterable<T> { 
    /**
     * Get member with specified key.
     * @param key specified key. It should match with type of the index and should be inclusive.
     * @return object with this value of the key or <code>null</code> if key nmot found
     * @exception StorageError(StorageError.KEY_NOT_UNIQUE) exception if there are more than 
     * one objects in the collection with specified value of the key.  
     */
    public T get(Object key);

    /**
     * Get members which key value belongs to the specified range.
     * Either from boundary, either till boundary either both of them can be <code>null</code>.
     * In last case the method returns all objects from the collection.
     * @param from low boundary. If <code>null</code> then low boundary is not specified.
     * Low boundary can be inclusive or exclusive. 
     * @param till high boundary. If <code>null</code> then high boundary is not specified.
     * High boundary can be inclusive or exclusive. 
     * @return array of objects which keys belongs to the specified interval, ordered by key value
     */
    public IPersistent[] get(Key from, Key till);

    /**
     * Add new member to collection
     * @param obj new member
     * @return <code>true</code> if object is successfully added in the index, 
     * <code>false</code> if collection was declared as unique and there is already member with such value
     * of the key in the collection. 
     */
    public boolean add(T obj);

    /**
     * Check if collections contains specified member
     * @return <code>true</code> if specified member belongs to the collection
     */
    public boolean contains(T member);

    /**
     * Remove member from collection
     * @param obj member to be removed
     * @exception StorageError(StorageError.KEY_NOT_FOUND) exception if there is no such key in the collection
     */
    public void remove(T obj);

    /**
     * Get number of objects in the collection
     * @return number of objects in the collection
     */
    public int size();
    
    /**
     * Remove all objects from the collection
     */
    public void clear();
    /**
     * Get all objects in the index as array ordered by index key.
     * @return array of objects in the index ordered by key value
     */
    public IPersistent[] toPersistentArray();

    /**
     * Get all objects in the index as array ordered by index key.
     * The runtime type of the returned array is that of the specified array.  
     * If the index fits in the specified array, it is returned therein.  
     * Otherwise, a new array is allocated with the runtime type of the 
     * specified array and the size of this index.<p>
     *
     * If this index fits in the specified array with room to spare
     * (i.e., the array has more elements than this index), the element
     * in the array immediately following the end of the index is set to
     * <tt>null</tt>.  This is useful in determining the length of this
     * index <i>only</i> if the caller knows that this index does
     * not contain any <tt>null</tt> elements.)<p>
     * @return array of objects in the index ordered by key value
     */
    public T[] toArray(T[] arr);

     /**
     * Get iterator for traversing all collection members.
     * @return collection iterator
     */
    public java.util.Iterator<T> iterator();
    /**
     * Get iterator for traversing collection members  with key belonging to the specified range. 
     * @param from low boundary. If <code>null</code> then low boundary is not specified.
     * Low boundary can be inclusive or exclusive. 
     * @param till high boundary. If <code>null</code> then high boundary is not specified.
     * High boundary can be inclusive or exclusive. 
     * @return selection iterator
     */
    public java.util.Iterator<T> iterator(Key from, Key till);

    /**
     * Get comparator used in this collection
     * @return collection comparator
     */
    public PersistentComparator<T> getComparator();
}
