package org.nachodb;

/**
 * Information about memory usage for the correspondent class. 
 * Instances of this class are created by Storage.getMemoryDump method.
 * Size of internal database structures (object index, memory allocation bitmap) is associated with 
 * <code>Storage</code> class. Size of class descriptors  - with <code>java.lang.Class</code> class.
 */
public class MemoryUsage {
    /**
     * Class of persistent object or Storage for database internal data
     */
    public Class cls;

    /**
     * Number of reachable instance of the particular class in the database.
     */
    public int nInstances;
    
    /**
     * Total size of all reachable instances
     */
    public long totalSize;

    /**
     * Real allocated size of all instances. Database allocates space for th objects using quantums,
     * for example object wilth size 25 bytes will use 32 bytes in the storage. 
     * In item associated with Storage class this field contains size of all allocated
     * space in the database (marked as used in bitmap)
     */
    public long allocatedSize;

    /**
     * MemoryUsage constructor
     */
    public MemoryUsage(Class cls) { 
        this.cls = cls;
    }
}