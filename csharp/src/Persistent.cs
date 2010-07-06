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

        public virtual int getOid()
        {
            return oid;
        }
        

        public virtual Storage Storage
        {
            get
            {
                return storage;
            }			
        }

        public virtual Storage getStorage() 
        { 
            return storage;
        }

        public virtual void  load()
        {
            if (storage != null)
            {
                storage.loadObject(this);
            }
        }
		
        public bool isRaw() 
        { 
            return (state & (int)ObjectState.RAW) != 0;
        } 
    
        public bool isModified() 
        { 
            return (state & (int)ObjectState.DIRTY) != 0;
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
            if ((state & (int)ObjectState.RAW) != 0)
            {
                throw new StorageError(StorageError.ErrorCode.ACCESS_TO_STUB);
            }
            if (storage != null) 
            {
                storage.storeObject(this);
                state &= ~(int)ObjectState.DIRTY;
            }
        }
		
        public void modify() 
        { 
            if ((state & (int)ObjectState.DIRTY) == 0 && storage != null) 
            { 
                if ((state & (int)ObjectState.RAW) != 0) 
                { 
                    throw new StorageError(StorageError.ErrorCode.ACCESS_TO_STUB);
                }
                storage.modifyObject(this);
                state |= (int)ObjectState.DIRTY;
            }
        }

        public virtual void  deallocate()
        {
            if (storage != null) 
            {
                storage.deallocateObject(this);
                storage = null;
            }
        }
		
        public virtual bool recursiveLoading()
        {
            return true;
        }
		
		
        public override bool Equals(System.Object o)
        {
            return o is Persistent && ((Persistent) o).Oid == oid;
        }
		
        public override int GetHashCode()
        {
            return oid;
        }
		
        public virtual void onLoad() 
        {
        }
        
        [NonSerialized()]
        internal Storage storage;
        [NonSerialized()]
        internal int oid;
        [NonSerialized()]
        internal int state;

        internal enum ObjectState 
        {
            RAW=1,
            DIRTY=2
        }
    }
}