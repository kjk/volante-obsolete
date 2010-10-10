package org.nachodb;

import java.util.*;
import java.lang.reflect.Field;

/**
 * Class use to project selected objects using relation field. 
 * For all selected objects (specified by array ort iterator), 
 * value of specified field (of IPersistent, array of IPersistent, Link or Relation type)
 * is inspected and all referenced object for projection (duplicate values are eliminated)
 */
public class Projection { 
    /**
     * Constructor of projection specified by class and field name of projected objects
     * @param type base class for selected objects
     * @param fieldName field name used to perform projection
     */
    public Projection(Class type, String fieldName) { 
        setProjectionField(type, fieldName);
    }

    /**
     * Default constructor of projection. This constructor should be used
     * only when you are going to derive your class from Projection and redefine
     * map method in it or sepcify type and fieldName later using setProjectionField
     * method
     */
    public Projection() {}

    /** 
     * Specify class of the projected objects and projection field name
     * @param type base class for selected objects
     * @param fieldName field name used to perform projection
     */
    public void setProjectionField(Class type, String fieldName) { 
        try { 
            field = type.getDeclaredField(fieldName);
            field.setAccessible(true);
        } catch (Exception x) { 
            throw new StorageError(StorageError.KEY_NOT_FOUND, x);
        }
    }

    /**
     * Project specified selection
     * @param selection array with selected object
     */
    public void project(IPersistent[] selection) { 
        for (int i = 0; i < selection.length; i++) { 
            map(selection[i]);
        }
    } 

    /**
     * Project specified object
     * @param obj selected object
     */
    public void project(IPersistent obj) { 
        map(obj);
    } 

    /**
     * Project specified selection
     * @param selection iterator specifying seleceted objects
     */
    public void project(Iterator selection) { 
        while (selection.hasNext()) { 
            map((IPersistent)selection.next());
        }
    } 

    /**
     * Join this projection with another projection.
     * Result of this join is set of objects present in both projections.
     */
    public void join(Projection prj) { 
        set.retainAll(prj.set);
    }

    /**
     * Get result of preceding project and join operations
     * @return array of objects
     */
    public IPersistent[] toArray() { 
        return (IPersistent[])set.toArray(new IPersistent[set.size()]);
    }

    /**
     * Get result of preceding project and join operations
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
     * @param arr destination array 
     * @return array of objects
     */
    public IPersistent[] toArray(IPersistent[] arr) { 
        return (IPersistent[])set.toArray(arr);
    }

    /**
     * Get number of objets in the result 
     */
    public int size() { 
        return set.size();
    }



    /**
     * Get iterator for result of preceding project and join operations
     * @return iterator
     */
    public Iterator iterator() { 
        return set.iterator();
    }

    /**
     * Reset projection - clear result of prceding project and join operations
     */
    public void reset() { 
        set.clear();
    }

    /**
     * Add object to the set
     * @param obj objet to be added
     */
    protected void add(IPersistent obj) { 
        if (obj != null) { 
            set.add(obj);
        }
    }

    /**
     * Get related objects for the object obj. 
     * It is possible to redifine this method in derived classes 
     * to provide application specific mapping
     * @param obj object from the selection
     */
    protected void map(IPersistent obj) {   
        if (field == null) { 
            add(obj);
        } else { 
            try { 
                Object o = field.get(obj);
                if (o instanceof Link) { 
                    IPersistent[] arr = ((Link)o).toArray();
                    for (int i = 0; i < arr.length; i++) { 
                        add(arr[i]);
                    }
                } else if (o instanceof IPersistent[]) { 
                    IPersistent[] arr = (IPersistent[])o;
                    for (int i = 0; i < arr.length; i++) { 
                        add(arr[i]);
                    }
                } else { 
                    add((IPersistent)o);
                }
            } catch (Exception x) { 
                throw new StorageError(StorageError.ACCESS_VIOLATION, x);
            }
        }
    }


    private HashSet set = new HashSet();
    private Field   field;
}
