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

        public virtual void Load()
        {
            if (storage != null)
            {
                storage.loadObject(this);
            }
        }
		
        public bool IsRaw() 
        { 
            return (state & (int)ObjectState.RAW) != 0;
        } 
    
        public bool IsModified() 
        { 
            return (state & (int)ObjectState.DIRTY) != 0;
        } 
 
        public bool IsPersistent()
        {
            return oid != 0;
        }
		
        public virtual void MakePersistent(Storage storage)
        {
            if (oid == 0)
            {
                storage.storeObject(this);
            }
        }
		
        public virtual void Store()
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
		
        public void Modify() 
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

        public virtual void Deallocate()
        {
            if (storage != null) 
            {
                storage.deallocateObject(this);
                storage = null;
            }
        }
		
        public virtual bool RecursiveLoading()
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
		
        public virtual void OnLoad() 
        {
        }
        
        public virtual void Invalidate() 
        {
	    state |= (int)ObjectState.RAW;
        }
        
        ~Persistent() 
        {
            if ((state & (int)ObjectState.DIRTY) != 0 && storage != null) 
            { 
                storage.storeFinalizedObject(this);
                state &= ~(int)ObjectState.DIRTY;
            }
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