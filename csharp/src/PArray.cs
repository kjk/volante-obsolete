namespace Volante
{
    using System;

    /// <summary>
    /// Common interface for all PArrays
    /// </summary> 
    public interface IGenericPArray
    {
        /// <summary> Get number of the array elements
        /// </summary>
        /// <returns>the number of related objects
        /// 
        /// </returns>
        int Size();

        /// <summary> Get OID of array element.
        /// </summary>
        /// <param name="i">index of the object in the relation
        /// </param>
        /// <returns>OID of the object (0 if array contains <code>null</code> reference)
        /// </returns>
        int GetOid(int i);

        /// <summary>
        /// Set owner objeet for this PArray. Owner is persistent object contaning this PArray.
        /// This method is mostly used by storage itself, but can also used explicityl by programmer if
        /// Parray component of one persistent object is assigned to component of another persistent object
        /// </summary>
        /// <param name="owner">link owner</param>
        void SetOwner(IPersistent owner);
    }

    /// <summary>Dynamically extended array of references to persistent objects.
    /// It is inteded to be used in classes using virtual properties to 
    /// access components of persistent objects. You can not use standard
    /// C# array here, instead you should use PArray class.
    /// PArray is created by Storage.createArray method
    /// </summary>
    public interface IPArray<T> : IGenericPArray, Link<T> where T : class,IPersistent
    {
    }
}