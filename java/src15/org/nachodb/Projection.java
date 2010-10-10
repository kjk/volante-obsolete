package org.nachodb;

import java.util.*;
import java.lang.reflect.Field;

/**
 * Class use to project selected objects using relation field. 
 * For all selected objects (specified by array ort iterator), 
 * value of specified field (of IPersistent, array of IPersistent, Link or Relation type)
 * is inspected and all referenced object for projection (duplicate values are eliminated)
 */
public class Projection<From extends IPersistent, To extends IPersistent> extends HashSet<To> { 
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
     * map method in it or specify type and fieldName later using setProjectionField
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
    public void project(From[] selection) { 
        for (int i = 0; i < selection.length; i++) { 
            map(selection[i]);
        }
    } 

    /**
     * Project specified object
     * @param obj selected object
     */
    public void project(From obj) { 
        map(obj);
    } 

    /**
     * Project specified selection
     * @param selection iterator specifying selected objects
     */
    public void project(Iterator<From> selection) { 
        while (selection.hasNext()) { 
            map(selection.next());
        }
    } 

    /**
     * Project specified selection
     * @param c selection iterator specifying selected objects
     */
    public void project(Collection<From> c) { 
        for (From o : c) { 
            map(o);
        }
    } 

    /**
     * Join this projection with another projection.
     * Result of this join is set of objects present in both projections.
     */
    public void join(Projection<From, To> prj) { 
        retainAll(prj);
    }

    /**
     * Get result of preceding project and join operations
     * @return array of objects
     */
    public IPersistent[] toPersistentArray() { 
        return (IPersistent[])toArray(new IPersistent[size()]);
    }

    /**
     * Reset projection - clear result of preceding project and join operations
     */
    public void reset() { 
        clear();
    }

    /**
     * Add object to the set
     * @param obj objet to be added
     */
    public boolean add(To obj) { 
        if (obj != null) { 
            return super.add(obj);
        }
        return false;
    }

    /**
     * Get related objects for the object obj. 
     * It is possible to redifine this method in derived classes 
     * to provide application specific mapping
     * @param obj object from the selection
     */
    protected void map(From obj) {   
        if (field == null) { 
            add((To)obj);
        } else { 
            try { 
                Object o = field.get(obj);
                if (o instanceof Link) { 
                    Object[] arr = ((Link)o).toArray();
                    for (int i = 0; i < arr.length; i++) { 
                        add((To)arr[i]);
                    }
                } else if (o instanceof Object[]) { 
                    Object[] arr = (Object[])o;
                    for (int i = 0; i < arr.length; i++) { 
                        add((To)arr[i]);
                    }
                } else { 
                    add((To)o);
                }
            } catch (Exception x) { 
                throw new StorageError(StorageError.ACCESS_VIOLATION, x);
            }
        }
    }

    private Field field;
}
