namespace Volante.Impl
{
    using Volante;

    public class PersistentStub : IPersistent
    {
        public virtual int Oid
        {
            get
            {
                return oid;
            }
        }

        public virtual IDatabase Database
        {
            get
            {
                return db;
            }
        }

        public virtual void Load()
        {
            throw new DatabaseException(DatabaseException.ErrorCode.ACCESS_TO_STUB);
        }

        public bool IsRaw()
        {
            return true;
        }

        public bool IsModified()
        {
            return false;
        }

        public bool IsDeleted()
        {
            return false;
        }

        public bool IsPersistent()
        {
            return true;
        }

        public virtual int MakePersistent(IDatabase db)
        {
            throw new DatabaseException(DatabaseException.ErrorCode.ACCESS_TO_STUB);
        }

        public virtual void Store()
        {
            throw new DatabaseException(DatabaseException.ErrorCode.ACCESS_TO_STUB);
        }

        public void Modify()
        {
            throw new DatabaseException(DatabaseException.ErrorCode.ACCESS_TO_STUB);
        }

        public virtual void Deallocate()
        {
            throw new DatabaseException(DatabaseException.ErrorCode.ACCESS_TO_STUB);
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
            throw new DatabaseException(DatabaseException.ErrorCode.ACCESS_TO_STUB);
        }

        internal PersistentStub(IDatabase db, int oid)
        {
            this.db = db;
            this.oid = oid;
        }

        public void AssignOid(IDatabase db, int oid, bool raw)
        {
            throw new DatabaseException(DatabaseException.ErrorCode.ACCESS_TO_STUB);
        }

        private IDatabase db;
        private int oid;
    }
}
