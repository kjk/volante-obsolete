namespace Perst
{
    using System;
	
    /// <summary> Interface for one-to-many relation. There are two types of relations:
    /// embedded (when references to the relarted obejcts are stored in lreation
    /// owner obejct itself) and stanalone (when relation is separate object, which contains
    /// the reference to the relation owner and relation members). Both kinds of relations
    /// implements Link interface. Embedded relation is created by Storage.createLink method
    /// and standalone relation is represented by Relation persistent class created by
    /// Storage.createRelation method.
    /// </summary>
    public interface Link
    {
        /// <summary> Get number of the linked objects 
        /// </summary>
        /// <returns>the number of related objects
        /// 
        /// </returns>
        int size();
        /// <summary> Get related object by index
        /// </summary>
        /// <param name="i">index of the object in the relation
        /// </param>
        /// <returns>referenced object
        /// 
        /// </returns>
        IPersistent get(int i);
        /// <summary> Get related object by index without loading it.
        /// Returned object can be used only to get it OID or to compare with other objects using
        /// <code>equals</code> method
        /// </summary>
        /// <param name="i">index of the object in the relation
        /// </param>
        /// <returns>stub representing referenced object
        /// 
        /// </returns>
        IPersistent getRaw(int i);
        /// <summary> Replace i-th element of the relation
        /// </summary>
        /// <param name="i">index in the relartion
        /// </param>
        /// <param name="obj">object to be included in the relation     
        /// 
        /// </param>
        void  set(int i, IPersistent obj);
        /// <summary> Remove object with specified index from the relation
        /// </summary>
        /// <param name="i">index in the relartion
        /// 
        /// </param>
        void  remove(int i);
        /// <summary> Insert new object in the relation
        /// </summary>
        /// <param name="i">insert poistion, should be in [0,size()]
        /// </param>
        /// <param name="obj">object inserted in the relation
        /// 
        /// </param>
        void  insert(int i, IPersistent obj);
        /// <summary> Add new object to the relation
        /// </summary>
        /// <param name="obj">object inserted in the relation
        /// 
        /// </param>
        void  add(IPersistent obj);
        /// <summary> Add all elements of the array to the relation
        /// </summary>
        /// <param name="arr">array of obects which should be added to the relation
        /// 
        /// </param>
        void  addAll(IPersistent[] arr);
        /// <summary> Add specified elements of the array to the relation
        /// </summary>
        /// <param name="arr">array of obects which should be added to the relation
        /// </param>
        /// <param name="from">index of the first element in the array to be added to the relation
        /// </param>
        /// <param name="length">number of elements in the array to be added in the relation
        /// 
        /// </param>
        void  addAll(IPersistent[] arr, int from, int length);
        /// <summary> Add all object members of the other relation to this relation
        /// </summary>
        /// <param name="link">another relation
        /// 
        /// </param>
        void  addAll(Link link);
        /// <summary> Get relation members as array of obejct
        /// </summary>
        /// <param name="array">of object with relation members
        /// 
        /// </param>
        IPersistent[] toArray();
        /// <summary> Checks if relation contains specified object
        /// </summary>
        /// <param name="obj">specified object
        /// 
        /// </param>
        bool contains(IPersistent obj);
        /// <summary> Get index of the specified object in the relation
        /// </summary>
        /// <param name="obj">specified object
        /// </param>
        /// <returns>zero based index of the object or -1 if object is not in the relation
        /// 
        /// </returns>
        int indexOf(IPersistent obj);
        /// <summary> Remove all members from the relation
        /// </summary>
        void  clear();
    }
}