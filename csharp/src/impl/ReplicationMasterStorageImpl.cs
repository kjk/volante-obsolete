#if WITH_REPLICATION
namespace Volante.Impl
{
    using Volante;

    public class ReplicationMasterDatabaseImpl : DatabaseImpl, ReplicationMasterDatabase
    {
        public ReplicationMasterDatabaseImpl(string[] hosts, int asyncBufSize)
        {
            this.hosts = hosts;
            this.asyncBufSize = asyncBufSize;
        }

        public override void Open(IFile file, int cacheSizeInBytes)
        {
            base.Open(asyncBufSize != 0
                ? (ReplicationMasterFile)new AsyncReplicationMasterFile(this, file, asyncBufSize)
                : new ReplicationMasterFile(this, file),
                cacheSizeInBytes);
        }

        public int GetNumberOfAvailableHosts()
        {
            return ((ReplicationMasterFile)pool.file).GetNumberOfAvailableHosts();
        }

        internal string[] hosts;
        internal int asyncBufSize;
    }
}
#endif
