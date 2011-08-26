using System;

namespace Volante
{
    /// <summary>
    /// Information about memory usage for one type. 
    /// Instances of this class are created by IDatabase.GetMemoryUsage method.
    /// Size of internal database structures (object index,* memory allocation bitmap) is associated with 
    /// <code>Database</code> class. Size of class descriptors  - with <code>System.Type</code> class.
    /// </summary>
    public class TypeMemoryUsage
    {
        /// <summary>
        /// Class of persistent object or Database for database internal data
        /// </summary>
        public Type Type;

        /// <summary>
        /// Number of reachable instance of the particular class in the database.
        /// </summary>
        public int Count;

        /// <summary>
        /// Total size of all reachable instances
        /// </summary>
        public long TotalSize;

        /// <summary>
        /// Real allocated size of all instances. Database allocates space for th objects using quantums,
        /// for example object wilth size 25 bytes will use 32 bytes in the db.
        /// In item associated with Database class this field contains size of all allocated
        /// space in the database (marked as used in bitmap) 
        /// </summary>
        public long AllocatedSize;

        /// <summary>
        /// TypeMemoryUsage constructor
        /// </summary>
        public TypeMemoryUsage(Type type)
        {
            this.Type = type;
        }
    }
}
