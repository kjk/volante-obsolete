namespace NachoDB
{
    using System;
    using System.Runtime.InteropServices;
    using System.ComponentModel;
    using System.Diagnostics;
	
    /// <summary>Base class for context bound object with provided
    /// transparent persistence. Objects derived from this class and marked with
    /// TransparentPresistence attribute automatically on demand load their 
    /// content from the database and also automatically detect object modification.
    /// </summary>
    public abstract class PersistentContext : ContextBoundObject, IPersistent
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
            if (oid != 0 && (state & ObjectState.RAW) != 0)
            {
                storage.loadObject(this);
            }
        }
		
        public bool IsRaw() 
        { 
            return (state & ObjectState.RAW) != 0;
        } 
    
        public bool IsDeleted() 
        { 
            return (state & ObjectState.DELETED) != 0;
        } 

        public bool IsModified() 
        { 
            return (state & ObjectState.DIRTY) != 0;
        } 
 
        public bool IsPersistent()
        {
            return oid != 0;
        }
		
        public virtual int MakePersistent(Storage storage)
        {
            if (oid == 0)
            {
                storage.MakePersistent(this);
            }
            return oid;
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
                Debug.Assert((state & ObjectState.DELETED) == 0);
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
                state = 0;
                oid = 0;
            }
        }
		
        public virtual bool RecursiveLoading()
        {
            return false;
        }
		
		
        public override bool Equals(System.Object o)
        {
            return o is IPersistent && ((IPersistent) o).Oid == oid;
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
        
        protected PersistentContext() {}
        
        protected PersistentContext(Storage storage) 
        {
            this.storage = storage;
        }

        ~PersistentContext() 
        {
            if ((state & ObjectState.DIRTY) != 0 && oid != 0) 
            { 
                storage.storeFinalizedObject(this);
            }
            state = ObjectState.DELETED;
        }

        public void AssignOid(Storage storage, int oid, bool raw)
        {
            this.oid = oid;
            this.storage = storage;
            if (raw) 
            {
                state |= ObjectState.RAW;
            }
            else 
            { 
                state &= ~ObjectState.RAW;
            }
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
            DIRTY=2,
            DELETED=4
        }
    }
}
