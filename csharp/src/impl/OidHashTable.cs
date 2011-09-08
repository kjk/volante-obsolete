namespace Volante.Impl
{
    using Volante;

    public interface OidHashTable
    {
        bool Remove(int oid);
        void Put(int oid, IPersistent obj);
        IPersistent Get(int oid);
        void Flush();
        void Invalidate();
        void SetDirty(int oid);
        void ClearDirty(int oid);
    }
}
