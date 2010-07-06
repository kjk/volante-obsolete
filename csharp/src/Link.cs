namespace Perst
{
    using System;
#if USE_GENERICS
    using System.Collections.Generic;
#else
    using System.Collections;
#endif
	
    public interface GenericLink {}

    /// <summary> Interface for one-to-many relation. There are two types of relations:
    /// embedded (when references to the relarted obejcts are stored in lreation
    /// owner obejct itself) and stanalone (when relation is separate object, which contains
    /// the reference to the relation owner and relation members). Both kinds of relations
    /// implements Link interface. Embedded relation is created by Storage.createLink method
    /// and standalone relation is represented by Relation persistent class created by
    /// Storage.createRelation method.
    /// </summary>
#if USE_GENERICS
    public interface Link<T> : ICollection<T>, GenericLink where T:class,IPersistent
#else
    public interface Link : ICollection, GenericLink
#endif
    {
        /// <summary> Get number of the linked objects 
        /// </summary>
        /// <returns>the number of related objects
        /// 
        /// </returns>
        int Size();

        /// <summary>Number of the linked objects 
        /// </summary>
        int Length {
             get;
             set;
        }        
        
        /// <summary> Access element by index
        /// </summary>
#if USE_GENERICS
        T this[int i]
#else
        IPersistent this[int i]
#endif
        {
             get;
             set;
        }       

        /// <summary> Get related object by index
        /// </summary>
        /// <param name="i">index of the object in the relation
        /// </param>
        /// <returns>referenced object
        /// 
        /// </returns>
#if USE_GENERICS
        T Get(int i);
#else
        IPersistent Get(int i);
#endif

        /// <summary> Get related object by index without loading it.
        /// Returned object can be used only to get it OID or to compare with other objects using
        /// <code>equals</code> method
        /// </summary>
        /// <param name="i">index of the object in the relation
        /// </param>
        /// <returns>stub representing referenced object
        /// 
        /// </returns>
        IPersistent GetRaw(int i);

        /// <summary> Replace i-th element of the relation
        /// </summary>
        /// <param name="i">index in the relartion
        /// </param>
        /// <param name="obj">object to be included in the relation     
        /// 
        /// </param>
#if USE_GENERICS
        void  Set(int i, T obj);
#else
        void  Set(int i, IPersistent obj);
#endif

        /// <summary> Remove object with specified index from the relation
        /// </summary>
        /// <param name="i">index in the relartion
        /// 
        /// </param>
        void  Remove(int i);

#if !USE_GENERICS
        /// <summary> Remove object from the relation
        /// </summary>
        /// <param name="obj">object to be removed
        /// </param>
        /// <returns><code>true</code> if member was successfully removed or <code>false</code> if member is not found</returns>
        bool Remove(IPersistent obj);
#endif

        /// <summary> Insert new object in the relation
        /// </summary>
        /// <param name="i">insert poistion, should be in [0,size()]
        /// </param>
        /// <param name="obj">object inserted in the relation
        /// 
        /// </param>
#if USE_GENERICS
        void  Insert(int i, T obj);
#else
        void  Insert(int i, IPersistent obj);
#endif

#if !USE_GENERICS
        /// <summary> Add new object to the relation
        /// </summary>
        /// <param name="obj">object inserted in the relation
        /// 
        /// </param>
        void  Add(IPersistent obj);
#endif

        /// <summary> Add all elements of the array to the relation
        /// </summary>
        /// <param name="arr">array of obects which should be added to the relation
        /// 
        /// </param>
#if USE_GENERICS
        void  AddAll(T[] arr);
#else
        void  AddAll(IPersistent[] arr);
#endif

        /// <summary> Add specified elements of the array to the relation
        /// </summary>
        /// <param name="arr">array of obects which should be added to the relation
        /// </param>
        /// <param name="from">index of the first element in the array to be added to the relation
        /// </param>
        /// <param name="length">number of elements in the array to be added in the relation
        /// 
        /// </param>
#if USE_GENERICS
        void  AddAll(T[] arr, int from, int length);
#else
        void  AddAll(IPersistent[] arr, int from, int length);
#endif

        /// <summary> Add all object members of the other relation to this relation
        /// </summary>
        /// <param name="link">another relation
        /// 
        /// </param>
#if USE_GENERICS
        void  AddAll(Link<T> link);
#else
        void  AddAll(Link link);
#endif

        /// <summary> Get relation members as array of objects
        /// </summary>
        /// <returns>created array</returns>
#if USE_GENERICS
        T[] ToArray();
#else
        IPersistent[] ToArray();
#endif

        /// <summary> 
        /// Return array with relation members. Members are not loaded and 
        /// size of the array can be greater than actual number of members. 
        /// </summary>
        /// <returns>array of object with relation members used in implementation of Link class
        /// </returns>
        Array ToRawArray(); 


        /// <summary> Get relation members as array with specifed element type
        /// </summary>
        /// <param name="elemType">element type of created array</param>
        /// <returns>created array</returns>
        Array ToArray(Type elemType);

#if !USE_GENERICS
        /// <summary> Checks if relation contains specified object
        /// </summary>
        /// <param name="obj">specified object
        /// 
        /// </param>
        bool Contains(IPersistent obj);
#endif

        /// <summary>Check if i-th element of Link is the same as specified obj
        /// </summary>
        /// <param name="i"> element index</param>
        /// <param name="obj">specified object</param>
        /// <returns><code>true</code> if i-th element of Link reference the same object as "obj"</returns>
#if USE_GENERICS
        bool ContainsElement(int i, T obj);
#else
        bool ContainsElement(int i, IPersistent obj);
#endif

        /// <summary> Get index of the specified object in the relation
        /// </summary>
        /// <param name="obj">specified object
        /// </param>
        /// <returns>zero based index of the object or -1 if object is not in the relation
        /// 
        /// </returns>
#if USE_GENERICS
        int IndexOf(T obj);
#else
        int IndexOf(IPersistent obj);
#endif

#if !USE_GENERICS
        /// <summary> Remove all members from the relation
        /// </summary>
        void  Clear();
#endif

        /// <summary>
        /// Replace all direct references to linked objects with stubs. 
        /// This method is needed tyo avoid memory exhaustion in case when 
        /// there is a large numebr of objectys in databasse, mutually
        /// refefencing each other (each object can directly or indirectly 
        /// be accessed from other objects).
        /// </summary>
        void Unpin();     
     
       /// <summary>
       /// Replace references to elements with direct references.
       /// It will impove spped of manipulations with links, but it can cause
       /// recursive loading in memory large number of objects and as a result - memory
       /// overflow, because garabge collector will not be able to collect them
       /// </summary>
       void Pin();     
    }
}