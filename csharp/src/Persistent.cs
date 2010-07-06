namespace Perst
{
    using System;
    using System.Runtime.InteropServices;
	
    /// <summary> Base class for all persistent capable objects
    /// </summary>
    public class Persistent : IPersistent
    {
        public virtual int Oid
        {
            get
            {
                return oid;
            }
			
        }
        public virtual Storage Storage
        {
            get
            {
                return storage;
            }
			
        }
        public virtual void  load()
        {
            if (raw)
            {
                storage.loadObject(this);
            }
        }
		
        public bool isRaw()
        {
            return raw;
        }
		
        public bool isPersistent()
        {
            return oid != 0;
        }
		
        public virtual void  makePersistent(Storage storage)
        {
            if (oid == 0)
            {
                storage.storeObject(this);
            }
        }
		
        public virtual void  store()
        {
            if (raw)
            {
                throw new StorageError(StorageError.ErrorCode.ACCESS_TO_STUB);
            }
            storage.storeObject(this);
        }
		
		
        public virtual void  deallocate()
        {
            storage.deallocateObject(this);
        }
		
        public virtual bool recursiveLoading()
        {
            return true;
        }
		
		
        public  override bool Equals(System.Object o)
        {
            return o is Persistent && ((Persistent) o).Oid == oid;
        }
		
        public override int GetHashCode()
        {
            return oid;
        }
		
        [NonSerialized()]
        internal Storage storage;
        [NonSerialized()]
        internal int oid;
        [NonSerialized()]
        internal bool raw;
    }
}