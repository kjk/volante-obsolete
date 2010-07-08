namespace NachoDB
{
    using System;
    using System.Runtime.InteropServices;
    using System.Diagnostics;
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
            if (oid != 0 && (state & ObjectState.RAW) != 0)
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
   
        public bool IsDeleted() 
        { 
            return (state & ObjectState.DELETED) != 0;
        } 

        public bool IsPersistent()
        {
            return oid != 0;
        }
		
        public virtual int MakePersistent(Storage storage)
        {
            return (oid != 0) ? oid : storage.MakePersistent(this);
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
            return true;
        }
		
		
        public override bool Equals(object o)
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
            state &= ~ObjectState.DIRTY;
            state |= ObjectState.RAW;
        }

        internal protected Persistent() {}

        protected Persistent(Storage storage) 
        {
            this.storage = storage;
        } 

        ~Persistent() 
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
        internal protected Storage storage;
        [NonSerialized()]
        internal protected int oid;
        [NonSerialized()]
        internal protected ObjectState state;

        [Flags]
        internal protected enum ObjectState 
        {
            RAW=1,
            DIRTY=2,
            DELETED=4
        }
    }
}