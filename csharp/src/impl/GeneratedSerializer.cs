namespace Volante.Impl
{
    using Volante;
    using System.Text;

    public interface GeneratedSerializer
    {
        IPersistent newInstance();
        int pack(DatabaseImpl store, IPersistent obj, ByteBuffer buf);
        void unpack(DatabaseImpl store, IPersistent obj, byte[] body, bool recursiveLoading);
    }
}
