namespace Volante.Impl
{
    using Volante;
    using System.Text;

    public interface GeneratedSerializer
    {
        IPersistent newInstance();
        int pack(StorageImpl store, IPersistent obj, ByteBuffer buf);
        void unpack(StorageImpl store, IPersistent obj, byte[] body, bool recursiveLoading, Encoding encoding);
    }
}
