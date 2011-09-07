namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    /// <summary> Interface of object spatial index.
    /// Spatial index is used to allow fast selection of spatial objects belonging to the specified rectangle.
    /// Spatial index is implemented using Guttman R-Tree with quadratic split algorithm.
    /// </summary>
    public interface ISpatialIndexR2<T> : IPersistent, IResource, ICollection<T> where T : class,IPersistent
    {
        /// <summary>
        /// Find all objects located in the selected rectangle
        /// </summary>
        /// <param name="r">selected rectangle
        /// </param>
        /// <returns>array of objects which enveloping rectangle intersects with specified rectangle
        /// </returns>             
        T[] Get(RectangleR2 r);

        /// <summary>
        /// Put new object in the index. 
        /// </summary>
        /// <param name="r">enveloping rectangle for the object
        /// </param>
        /// <param name="obj"> object associated with this rectangle. Object can be not yet persistent, in this case
        /// its forced to become persistent by assigning oid to it.
        /// </param>
        void Put(RectangleR2 r, T obj);

        /// <summary>
        /// Remove object with specified enveloping rectangle from the tree.
        /// </summary>
        /// <param name="r">enveloping rectangle for the object
        /// </param>
        /// <param name="obj">object removed from the index
        /// </param>
        /// <exception  cref="Volante.DatabaseException">DatabaseException(DatabaseException.KEY_NOT_FOUND) exception 
        /// if there is no such key in the index
        /// </exception>
        void Remove(RectangleR2 r, T obj);

        /// <summary>
        /// Get wrapping rectangle 
        /// </summary>
        /// <returns>Minimal rectangle containing all rectangles in the index
        /// If index is empty <i>empty rectangle</i> (double.MaxValue, double.MaxValue, double.MinValue, double.MinValue)
        /// is returned.
        /// </returns>
        RectangleR2 WrappingRectangle
        {
            get;
        }

        /// <summary>
        /// Get enumerator for objects located in the selected rectangle
        /// </summary>
        /// <param name="rect">Selected rectangle</param>
        /// <returns>enumerable collection for objects which enveloping rectangle overlaps with specified rectangle
        /// </returns>
        IEnumerable<T> Overlaps(RectangleR2 rect);

        /// <summary>
        /// Get dictionary enumerator for objects located in the selected rectangle
        /// </summary>
        /// <param name="rect">Selected rectangle</param>
        /// <returns>dictionary enumerator for objects which enveloping rectangle overlaps with specified rectangle
        /// </returns>
        IDictionaryEnumerator GetDictionaryEnumerator(RectangleR2 rect);

        /// <summary>
        /// Get dictionary enumerator for all objects in the index
        /// </summary>
        /// <returns>dictionary enumerator for all objects in the index
        /// </returns>
        IDictionaryEnumerator GetDictionaryEnumerator();
    }
}

