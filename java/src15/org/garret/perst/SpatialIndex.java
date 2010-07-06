package org.garret.perst;

import java.util.ArrayList;

/**
 * Interface of object spatial index.
 * Spatial index is used to allow fast selection of spatial objects belonging to the specified rectangle.
 * Spatial index is implemented using Guttman R-Tree with quadratic split algorithm.
 */
public interface SpatialIndex<T extends IPersistent> extends IPersistent, IResource { 
    /**
     * Find all objects located in the selected rectangle
     * @param r selected rectangle
     * @return array list of objects which enveloping rectangle intersects with specified rectangle
     */
    public IPersistent[] get(Rectangle r);

    /**
     * Find all objects located in the selected rectangle
     * @param r selected rectangle
     * @return array list of objects which enveloping rectangle intersects with specified rectangle
     */
    public ArrayList<T> getList(Rectangle r);
    
    /**
     * Put new object in the index. 
     * @param r enveloping rectangle for the object
     * @param obj object associated with this rectangle. Object can be not yet persistent, in this case
     * its forced to become persistent by assigning OID to it.
     */
    public void put(Rectangle r, T obj);

    /**
     * Remove object with specified enveloping rectangle from the tree.
     * @param r enveloping rectangle for the object
     * @param obj object removed from the index
     * @exception StorageError(StorageError.KEY_NOT_FOUND) exception if there is no such key in the index
     */
    public void remove(Rectangle r, T obj);

    /**
     * Get number of objects in the index
     * @return number of objects in the index
     */
    public int  size();
    
    /**
     * Remove all objects from the index
     */
    public void clear();
}
