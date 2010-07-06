namespace Perst
{
    using System;
	
    public interface GenericPArray {}

    /// <summary>Dynamically extended array of reference to persistent objects.
    /// It is inteded to be used in classes using virtual properties to 
    /// access components of persistent objects. You can not use standard
    /// C# array here, instead you should use PArray class.
    /// PArray is created by Storage.createArray method
    /// </summary>
#if USE_GENERICS
    public interface PArray<T> : GenericPArray, Link<T> where T:class,IPersistent
#else
    public interface PArray : GenericPArray, Link
#endif
    {
        /// <summary> Get OID of arary element.
        /// </summary>
        /// <param name="i">index of the object in the relation
        /// </param>
        /// <returns>OID of the object (0 if array contains <code>null</code> reference)
        /// </returns>
        int GetOid(int i);
    }
}