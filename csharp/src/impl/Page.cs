namespace Perst.Impl
{
    using System;
    using Perst;
	
    class Page:LRU, IComparable
    {
        internal Page collisionChain;
        internal int accessCount;
        internal int writeQueueIndex;
        internal int state;
        internal long offs;
        internal byte[] data;
		
        internal static int psDirty = 0x01; // page has been modified
        internal static int psRaw   = 0x02; // page is loaded from the disk
        internal static int psWait  = 0x04; // other thread(s) wait load operation completion
		
        internal const int pageBits = 12;
        internal const int pageSize = 1 << pageBits;
		
        public virtual int CompareTo(Object o)
        {
            long po = ((Page) o).offs;
            return offs < po ? -1 : offs == po ? 0 : 1;
        }
		
        internal Page()
        {
            data = new byte[pageSize];
        }
    }
}