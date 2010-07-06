namespace Perst
{
    using System;
    using System.Collections;
	
    /// <summary> Interface of object spatial index.
    /// Spatial index is used to allow fast selection of spatial objects belonging to the specified rectangle.
    /// Spatial index is implemented using Guttman R-Tree with quadratic split algorithm.
    /// </summary>
    public interface SpatialIndex : IPersistent, IResource
    {
        /// <summary>
        /// Find all objects located in the selected rectangle
        /// </summary>
        /// <param name="r">selected rectangle
        /// </param>
        /// <returns>array of objects which enveloping rectangle intersects with specified rectangle
        /// </returns>             
        IPersistent[] get(Rectangle r);
    
        /// <summary>
        /// Put new object in the index. 
        /// </summary>
        /// <param name="r">enveloping rectangle for the object
        /// </param>
        /// <param name="obj"> object associated with this rectangle. Object can be not yet persistent, in this case
        /// its forced to become persistent by assigning OID to it.
        /// </param>
        void put(Rectangle r, IPersistent obj);

        /// <summary>
        /// Remove object with specified enveloping rectangle from the tree.
        /// </summary>
        /// <param name="r">enveloping rectangle for the object
        /// </param>
        /// <param name="obj">object removed from the index
        /// </param>
        /// <exception  cref="StorageError">StorageError(StorageError.KEY_NOT_FOUND) exception if there is no such key in the index
        /// </exception>
        void remove(Rectangle r, IPersistent obj);

        /// <summary>
        /// Get number of objects in the index
        /// </summary>
        /// <returns>number of objects in the index
        /// </returns>
        int  size();
    
        /// <summary>
        /// Remove all objects from the index
        /// </summary>
        void clear();
    }
}
