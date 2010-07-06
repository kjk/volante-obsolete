namespace Perst
{
    using System;
    using StorageImpl = Perst.Impl.StorageImpl;
	
    /// <summary> Storage factory
    /// </summary>
    public class StorageFactory
    {
        /// <summary> Get instance of storage factory.
        /// So new storages should be create in application in the following way:
        /// <code>StorageFactory.Instance.createStorage()</code>
        /// </summary>
        public static StorageFactory Instance
        {
            get
            {
                return instance;
            }
			
        }

        /// <summary> Get instance of storage factory.
        /// So new storages should be create in application in the following way:
        /// <code>StorageFactory.getInstance().createStorage()</code>
        /// </summary>
        /// <returns>instance of the storage factory
        /// 
        /// </returns>
        public static StorageFactory getInstance() 
        {
            return instance;
        }

        /// <summary> Create new instance of the storage
        /// </summary>
        /// <param name="new">instance of the storage (unopened,you should explicitely invoke open method)
        /// 
        /// </param>
        public virtual Storage createStorage()
        {
#if COMPACT_NET_FRAMEWORK
            return new StorageImpl(System.Reflection.Assembly.GetCallingAssembly());
#else
            return new StorageImpl();
#endif
        }
		
		
        protected internal static StorageFactory instance = new StorageFactory();
    }
	
}