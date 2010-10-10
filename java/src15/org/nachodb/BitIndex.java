package org.nachodb;

import java.util.*;

/**
 * Interface of bit index.
 * Bit index allows to efficiently search object with specified 
 * set of properties. Each object has associated mask of 32 bites. 
 * Meaning of bits is application dependent. Usually each bit stands for
 * some binary or boolean property, for example "sex", but it is possible to 
 * use group of bits to represent enumerations with more possible values.
 */
public interface BitIndex<T extends IPersistent> extends IPersistent, IResource, Collection<T> { 
    /**
     * Get properties of specified object
     * @param obj object which properties are requested
     * @return bit mask associated with this objects
     * @exception StorageError(StorageError.KEY_NOT_FOUND) exception if there is no object in the index
     */
    public int get(T obj);

    /**
     * Put new object in the index. If such object already exists in index, then its
     * mask will be rewritten 
     * @param obj object placed in the index. Object can be not yet persistent, in this case
     * its forced to become persistent by assigning OID to it.
     * @param mask bit mask associated with this objects
     */
    public void put(T obj, int mask);

    /**
     * Remove object from the index 
     * @param obj object removed from the index
     * @exception StorageError(StorageError.KEY_NOT_FOUND) exception if there is no such key in the index
     */
    public void remove(T obj);

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
     * Get iterator for selecting objects with specified properties.
     * To select all record this method should be invoked with (0, 0) parameters
     * @param set bitmask specifying bits which should be set (1)
     * @param clear bitmask specifying bits which should be cleared (0)
     * @return selection iterator
     */
    public IterableIterator<T> iterator(int set, int clear);
}


