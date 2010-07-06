namespace Perst
{
    using System;
    using System.Threading;
	
    /// <summary>Base class for persistent capable objects supporting locking
    /// </summary>
    public class PersistentResource : Persistent, IResource 
    {
        public void sharedLock()    
        {
            lock (this) 
            { 
                Thread currThread = Thread.CurrentThread;
                while (true) 
                { 
                    if (owner == currThread) 
                    { 
                        nWriters += 1;
                        break;
                    } 
                    else if (nWriters == 0) 
                    { 
                        nReaders += 1;
                        break;
                    } 
                    else 
                    { 
                        Monitor.Wait(this);
                    }
                }
            }
        }
                    
        public bool sharedLock(long timeout) 
        {
            Thread currThread = Thread.CurrentThread;
            DateTime startTime = DateTime.Now;
            TimeSpan ts = TimeSpan.FromMilliseconds(timeout);
            lock (this) 
            { 
                while (true) 
                { 
                    if (owner == currThread) 
                    { 
                        nWriters += 1;
                        return true;
                    } 
                    else if (nWriters == 0) 
                    { 
                        nReaders += 1;
                        return true;
                    } 
                    else 
                    { 
                        DateTime currTime = DateTime.Now;
                        if (startTime + ts <= currTime) 
                        { 
                            return false;
                        }
                        Monitor.Wait(this, startTime + ts - currTime);
                    }
                }
            } 
        }
    
                    
        public void exclusiveLock() 
        {
            Thread currThread = Thread.CurrentThread;
            lock (this)
            { 
                while (true) 
                { 
                    if (owner == currThread) 
                    { 
                        nWriters += 1;
                        break;
                    } 
                    else if (nReaders == 0 && nWriters == 0) 
                    { 
                        nWriters = 1;
                        owner = currThread;
                        break;
                    } 
                    else 
                    { 
                        Monitor.Wait(this);
                    }
                }
            } 
        }
                    
        public bool exclusiveLock(long timeout) 
        {
            Thread currThread = Thread.CurrentThread;
            TimeSpan ts = TimeSpan.FromMilliseconds(timeout);
            DateTime startTime = DateTime.Now;
            lock (this) 
            { 
                while (true) 
                { 
                    if (owner == currThread) 
                    { 
                        nWriters += 1;
                        return true;
                    } 
                    else if (nReaders == 0 && nWriters == 0) 
                    { 
                        nWriters = 1;
                        owner = currThread;
                        return true;
                    } 
                    else 
                    { 
                        DateTime currTime = DateTime.Now;
                        if (startTime + ts <= currTime) 
                        { 
                            return false;
                        }
                        Monitor.Wait(this, startTime + ts - currTime);
                    }
                }
            } 
        }
        public void unlock() 
        {
            lock (this) 
            { 
                if (nWriters != 0) 
                { 
                    if (--nWriters == 0) 
                    { 
                        owner = null;
                        Monitor.PulseAll(this);
                    }
                } 
                else 
                { 
                    if (--nReaders == 0) 
                    { 
                        Monitor.PulseAll(this);
                    }
                }
            }
        }

        [NonSerialized()]
        private Thread owner;
        [NonSerialized()]
        private int    nReaders;
        [NonSerialized()]
        private int    nWriters;
    }
}
