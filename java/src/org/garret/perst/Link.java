package org.garret.perst;

/**
 * Interface for one-to-many relation. There are two types of relations:
 * embedded (when references to the relarted obejcts are stored in relation
 * owner obejct itself) and stanalone (when relation is separate object, which contains
 * the reference to the relation owner and relation members). Both kinds of relations
 * implements Link interface. Embedded relation is created by Storage.createLink method
 * and standalone relation is represented by Relation persistent class created by
 * Storage.createRelation method.
 */
public interface Link {
    /**
     * Get number of the linked objects 
     * @return the number of related objects
     */
    public int size();
    
    /**
     * Get related object by index
     * @param i index of the object in the relation
     * @return referenced object
     */
    public IPersistent get(int i);

    /**
     * Get related object by index without loading it.
     * Returned object can be used only to get it OID or to compare with other objects using
     * <code>equals</code> method
     * @param i index of the object in the relation
     * @return stub representing referenced object
     */
    public IPersistent getRaw(int i);

    /**
     * Replace i-th element of the relation
     * @param i index in the relartion
     * @param obj object to be included in the relation     
     */
    public void set(int i, IPersistent obj);

    /**
     * Remove object with specified index from the relation
     * @param i index in the relartion
     */
    public void remove(int i);

    /**
     * Insert new object in the relation
     * @param i insert poistion, should be in [0,size()]
     * @param obj object inserted in the relation
     */
    public void insert(int i, IPersistent obj);

    /**
     * Add new object to the relation
     * @param obj object inserted in the relation
     */
    public void add(IPersistent obj);

    /**
     * Add all elements of the array to the relation
     * @param arr array of obects which should be added to the relation
     */
    public void addAll(IPersistent[] arr);
    
    /**
     * Add specified elements of the array to the relation
     * @param arr array of obects which should be added to the relation
     * @param from index of the first element in the array to be added to the relation
     * @param length number of elements in the array to be added in the relation
     */
    public void addAll(IPersistent[] arr, int from, int length);

    /**
     * Add all object members of the other relation to this relation
     * @param link another relation
     */
    public void addAll(Link link);

    /**
     * Get relation members as array of obejct
     * @return array of object with relation members
     */
    public IPersistent[] toArray();
    
    /**
     * Checks if relation contains specified object
     * @param obj specified object
     */
    public boolean contains(IPersistent obj);

    /**
     * Get index of the specified object in the relation
     * @param obj specified object
     * @return zero based index of the object or -1 if object is not in the relation
     */
    public int indexOf(IPersistent obj);

    /**
     * Remove all members from the relation
     */
    public void clear();

    /**
     * Get iterator through link members
     */
    public java.util.Iterator iterator();
}





