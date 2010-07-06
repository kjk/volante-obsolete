namespace Perst
{
    using System;
    using System.Runtime.InteropServices;
    using System.ComponentModel;
	
    /// <summary> Base class for persistent capable objects with marshal be reference semantic
    /// </summary>
    public abstract class MarshalByRefPersistent : MarshalByRefObject, IPersistent
    {
        [Browsable(false)]
        public virtual int Oid
        {
            get
            {
                return oid;
            }	
        }

        [Browsable(false)]
        public virtual Storage Storage
        {
            get
            {
                return storage;
            }			
        }

        public virtual void Load()
        {
            if (oid != 0)
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
            if ((state & ObjectState.DIRTY) == 0 && oid != 0) 
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
            if (oid != 0) 
            {
                storage.deallocateObject(this);
                storage = null;
                oid = 0;
                state = 0;
            }
        }
		
        public virtual bool RecursiveLoading()
        {
            return true;
        }
		
		
        public override bool Equals(System.Object o)
        {
            return o is Persistent && ((MarshalByRefPersistent) o).Oid == oid;
        }
		
        public override int GetHashCode()
        {
            return oid;
        }
		
        public virtual void OnLoad() 
        {
        }
        
        public virtual void OnStore() 
        {
        }
        
        public virtual void Invalidate() 
        {
            state |= ObjectState.RAW;
        }
        
        protected MarshalByRefPersistent() {}

        protected MarshalByRefPersistent(Storage storage) 
        {
            this.storage = storage;
        }


        ~MarshalByRefPersistent() 
        {
            if ((state & ObjectState.DIRTY) != 0 && oid != 0) 
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
