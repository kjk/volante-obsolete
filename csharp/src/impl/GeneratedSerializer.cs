namespace Perst.Impl
{
    using Perst;

	public interface GeneratedSerializer
	{
        IPersistent newInstance();
        int         pack(StorageImpl store, object obj, ByteBuffer buf);
        void        unpack(StorageImpl store, object obj, byte[] body, bool recursiveLoading);
    }
}
