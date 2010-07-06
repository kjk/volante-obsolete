namespace Perst
{
    using System;
    using System.Runtime.InteropServices;
#if !COMPACT_NET_FRAMEWORK
    using System.ComponentModel;
#endif
	
    /// <summary> Base class for all persistent capable objects
    /// </summary>
    public class Persistent : IPersistent
    {
#if !COMPACT_NET_FRAMEWORK
        [Browsable(false)]
#endif
        public virtual int Oid
        {
            get
            {
                return oid;
            }	
        }

#if !COMPACT_NET_FRAMEWORK
        [Browsable(false)]
#endif
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
            return (state & ObjectState.RAW) != 0;
        } 
    
        public bool IsModified() 
        { 
            return (state & ObjectState.DIRTY) != 0;
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
            if ((state & ObjectState.RAW) != 0)
            {
                throw new StorageError(StorageError.ErrorCode.ACCESS_TO_STUB);
            }
            if (storage != null) 
            {
                storage.storeObject(this);
                state &= ~ObjectState.DIRTY;
            }
        }
		
        public void Modify() 
        { 
            if ((state & ObjectState.DIRTY) == 0 && storage != null) 
            { 
                if ((state & ObjectState.RAW) != 0) 
                { 
                    throw new StorageError(StorageError.ErrorCode.ACCESS_TO_STUB);
                }
                storage.modifyObject(this);
                state |= ObjectState.DIRTY;
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
	        state |= ObjectState.RAW;
        }
        
        ~Persistent() 
        {
            if ((state & ObjectState.DIRTY) != 0 && storage != null) 
            { 
                storage.storeFinalizedObject(this);
                state &= ~ObjectState.DIRTY;
            }
        }

        public void AssignOid(Storage storage, int oid, bool raw)
        {
            this.oid = oid;
            this.storage = storage;
            state = raw ? ObjectState.RAW : 0;
        }


	    [NonSerialized()]
        Storage storage;
        [NonSerialized()]
        int oid;
        [NonSerialized()]
        ObjectState state;

        [Flags]
        enum ObjectState 
        {
            RAW=1,
            DIRTY=2
        }
    }
}