namespace Volante
{
    /// <summary>
    /// Database performing replication of changed pages to specified slave nodes.
    /// </summary>
    public interface ReplicationMasterDatabase : IDatabase
    {
        /// <summary>
        /// Get number of currently available slave nodes
        /// </summary>
        /// <returns>number of online replication slaves</returns>
        int GetNumberOfAvailableHosts();
    }
}
