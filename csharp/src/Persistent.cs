namespace Volante
{
    using System;
    using System.Runtime.InteropServices;
    using System.Diagnostics;
#if !CF
    using System.ComponentModel;
#endif

    /// <summary>Base class for all persistent capable objects
    /// </summary>
    public class Persistent : IPersistent
    {
#if !CF
        [Browsable(false)]
#endif
        public virtual int Oid
        {
            get
            {
                return oid;
            }
        }

#if !CF
        [Browsable(false)]
#endif
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
                db.loadObject(this);
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

        public virtual int MakePersistent(IDatabase db)
        {
            if (oid == 0)
                db.MakePersistent(this);
            return oid;
        }

        public virtual void Store()
        {
            if ((state & ObjectState.RAW) != 0)
                throw new DatabaseException(DatabaseException.ErrorCode.ACCESS_TO_STUB);

            if (db != null)
            {
                db.storeObject(this);
                state &= ~ObjectState.DIRTY;
            }
        }

        public void Modify()
        {
            if (((state & ObjectState.DIRTY) != 0) || (oid == 0))
                return;

            if ((state & ObjectState.RAW) != 0)
                throw new DatabaseException(DatabaseException.ErrorCode.ACCESS_TO_STUB);

            Debug.Assert((state & ObjectState.DELETED) == 0);
            db.modifyObject(this);
            state |= ObjectState.DIRTY;
        }

        public virtual void Deallocate()
        {
            if (0 == oid)
                return;

            db.deallocateObject(this);
            db = null;
            state = 0;
            oid = 0;
        }

        public virtual bool RecursiveLoading()
        {
            return true;
        }

        public override bool Equals(object o)
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
            state &= ~ObjectState.DIRTY;
            state |= ObjectState.RAW;
        }

        internal protected Persistent() { }

        protected Persistent(IDatabase db)
        {
            this.db = db;
        }

        ~Persistent()
        {
            if ((state & ObjectState.DIRTY) != 0 && oid != 0)
                db.storeFinalizedObject(this);

            state = ObjectState.DELETED;
        }

        public void AssignOid(IDatabase db, int oid, bool raw)
        {
            this.oid = oid;
            this.db = db;
            if (raw)
                state |= ObjectState.RAW;
            else
                state &= ~ObjectState.RAW;
        }

        [NonSerialized()]
        internal protected IDatabase db;
        [NonSerialized()]
        internal protected int oid;
        [NonSerialized()]
        internal protected ObjectState state;

        [Flags]
        internal protected enum ObjectState
        {
            RAW = 1,
            DIRTY = 2,
            DELETED = 4
        }
    }
}