namespace Volante
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
        public virtual IDatabase Database
        {
            get
            {
                return db;
            }
        }

        public virtual void Load()
        {
            if (oid != 0 && (state & ObjectState.RAW) != 0)
            {
                db.loadObject(this);
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

        public virtual int MakePersistent(IDatabase db)
        {
            if (oid == 0)
                db.MakePersistent(this);
            return oid;
        }

        public virtual void Store()
        {
            if ((state & ObjectState.RAW) != 0)
            {
                throw new DatabaseException(DatabaseException.ErrorCode.ACCESS_TO_STUB);
            }
            if (db != null)
            {
                db.storeObject(this);
                state &= ~ObjectState.DIRTY;
            }
        }

        public void Modify()
        {
            if ((state & ObjectState.DIRTY) == 0 && oid != 0)
            {
                if ((state & ObjectState.RAW) != 0)
                {
                    throw new DatabaseException(DatabaseException.ErrorCode.ACCESS_TO_STUB);
                }
                Debug.Assert((state & ObjectState.DELETED) == 0);
                db.modifyObject(this);
                state |= ObjectState.DIRTY;
            }
        }

        public virtual void Deallocate()
        {
            if (oid != 0)
            {
                db.deallocateObject(this);
                db = null;
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
            return o is IPersistent && ((IPersistent)o).Oid == oid;
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

        protected PersistentContext() { }

        protected PersistentContext(IDatabase db)
        {
            this.db = db;
        }

        ~PersistentContext()
        {
            if ((state & ObjectState.DIRTY) != 0 && oid != 0)
            {
                db.storeFinalizedObject(this);
            }
            state = ObjectState.DELETED;
        }

        public void AssignOid(IDatabase db, int oid, bool raw)
        {
            this.oid = oid;
            this.db = db;
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
        IDatabase db;
        [NonSerialized()]
        int oid;
        [NonSerialized()]
        ObjectState state;

        [Flags]
        enum ObjectState
        {
            RAW = 1,
            DIRTY = 2,
            DELETED = 4
        }
    }
}
