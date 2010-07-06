namespace Perst
{
    using System;
#if USE_GENERICS
    using System.Collections.Generic;
#endif
    using System.Collections;
	
    /// <summary> Interface of object spatial index.
    /// Spatial index is used to allow fast selection of spatial objects belonging to the specified rectangle.
    /// Spatial index is implemented using Guttman R-Tree with quadratic split algorithm.
    /// </summary>
#if USE_GENERICS
    public interface SpatialIndex<T> : IPersistent, IResource, ICollection<T> where T:class,IPersistent
#else
    public interface SpatialIndex : IPersistent, IResource, ICollection
#endif
    {
        /// <summary>
        /// Find all objects located in the selected rectangle
        /// </summary>
        /// <param name="r">selected rectangle
        /// </param>
        /// <returns>array of objects which enveloping rectangle intersects with specified rectangle
        /// </returns>             
#if USE_GENERICS
        T[] Get(Rectangle r);
#else
        IPersistent[] Get(Rectangle r);
#endif
    
        /// <summary>
        /// Put new object in the index. 
        /// </summary>
        /// <param name="r">enveloping rectangle for the object
        /// </param>
        /// <param name="obj"> object associated with this rectangle. Object can be not yet persistent, in this case
        /// its forced to become persistent by assigning OID to it.
        /// </param>
#if USE_GENERICS
        void Put(Rectangle r, T obj);
#else
        void Put(Rectangle r, IPersistent obj);
#endif

        /// <summary>
        /// Remove object with specified enveloping rectangle from the tree.
        /// </summary>
        /// <param name="r">enveloping rectangle for the object
        /// </param>
        /// <param name="obj">object removed from the index
        /// </param>
        /// <exception  cref="Perst.StorageError">StorageError(StorageError.KEY_NOT_FOUND) exception if there is no such key in the index
        /// </exception>
#if USE_GENERICS
        void Remove(Rectangle r, T obj);
#else
        void Remove(Rectangle r, IPersistent obj);
#endif

        /// <summary>
        /// Get number of objects in the index
        /// </summary>
        /// <returns>number of objects in the index
        /// </returns>
        int  Size();
    
#if !USE_GENERICS
        /// <summary>
        /// Remove all objects from the index
        /// </summary>
        void Clear();
#endif

        /// <summary>
        /// Get wrapping rectangle 
        /// </summary>
        /// <returns>Minimal rectangle containing all rectangles in the index     
        /// If index is empty <i>empty rectangle</i> (double.MaxValue, double.MaxValue, double.MinValue, double.MinValue)
        /// is returned.
        /// </returns>
        Rectangle WrappingRectangle 
        {
            get;
        }

        /// <summary>
        /// Get enumerator for objects located in the selected rectangle
        /// </summary>
        /// <param name="rect">Selected rectangle</param>
        /// <returns>enumerable collection for objects which enveloping rectangle overlaps with specified rectangle
        /// </returns>
#if USE_GENERICS
        IEnumerable<T> Overlaps(Rectangle r);
#else
        IEnumerable Overlaps(Rectangle r);
#endif

        /// <summary>
        /// Get dictionary enumerator for objects located in the selected rectangle
        /// </summary>
        /// <param name="rect">Selected rectangle</param>
        /// <returns>dictionary enumerator for objects which enveloping rectangle overlaps with specified rectangle
        /// </returns>
        IDictionaryEnumerator GetDictionaryEnumerator(Rectangle r);

        /// <summary>
        /// Get dictionary enumerator for all objects in the index
        /// </summary>
        /// <returns>dictionary enumerator for all objects in the index
        /// </returns>
        IDictionaryEnumerator GetDictionaryEnumerator();
    }
}

