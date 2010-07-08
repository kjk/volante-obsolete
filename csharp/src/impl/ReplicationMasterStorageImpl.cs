#if !OMIT_REPLICATION
namespace NachoDB.Impl    
{
    using NachoDB;

    public class ReplicationMasterStorageImpl : StorageImpl, ReplicationMasterStorage
    { 
        public ReplicationMasterStorageImpl(string[] hosts, int asyncBufSize) 
        { 
            this.hosts = hosts;
            this.asyncBufSize = asyncBufSize;
        }
    
        public override void Open(IFile file, int pagePoolSize) 
        {
            base.Open(asyncBufSize != 0 
                ? (ReplicationMasterFile)new AsyncReplicationMasterFile(this, file, asyncBufSize)
                : new ReplicationMasterFile(this, file),
                pagePoolSize);
        }

        public int GetNumberOfAvailableHosts() 
        { 
            return ((ReplicationMasterFile)pool.file).GetNumberOfAvailableHosts();
        }

        internal string[] hosts;
        internal int      asyncBufSize;
    }
}
#endif
