namespace Perst.Impl
{
    using System;
    using Perst;
	
    public class WeakHashTable : OidHashTable
    {
        internal Entry[] table;
        internal const float loadFactor = 0.75f;
        internal int count;
        internal int threshold;
		
        public WeakHashTable(int initialCapacity)
        {
            threshold = (int) (initialCapacity * loadFactor);
            if (initialCapacity != 0)
            {
                table = new Entry[initialCapacity];
            }
        }
		
        public bool remove(int oid)
        {
            lock(this)
            {
                Entry[] tab = table;
                int index = (oid & 0x7FFFFFFF) % tab.Length;
                for (Entry e = tab[index], prev = null; e != null; prev = e, e = e.next)
                {
                    if (e.oid == oid)
                    {
                        e.dirty = 0;
                        e.oref.Target = null;
                        count -= 1;
                        if (prev != null)
                        {
                            prev.next = e.next;
                        }
                        else
                        {
                            tab[index] = e.next;
                        }
                        return true;
                    }
                }
                return false;
            }
        }
		
        public void  put(int oid, IPersistent obj)
        {
            lock(this)
            {
                Entry[] tab = table;
                int index = (oid & 0x7FFFFFFF) % tab.Length;
                for (Entry e = tab[index]; e != null; e = e.next)
                {
                    if (e.oid == oid)
                    {
                        e.oref.Target = obj;
                        return ;
                    }
                }
                if (count >= threshold)
                {
                    // Rehash the table if the threshold is exceeded
                    rehash();
                    tab = table;
                    index = (oid & 0x7FFFFFFF) % tab.Length;
                }
				
                // Creates the new entry.
                tab[index] = new Entry(oid, new WeakReference(obj), tab[index]);
                count++;
            }
        }
		
        public IPersistent get(int oid)
        {
            while (true) 
            { 
                lock(this)
                {
                    Entry[] tab = table;
                    int index = (oid & 0x7FFFFFFF) % tab.Length;
                    for (Entry e = tab[index]; e != null; e = e.next)
                    {
                        if (e.oid == oid)
                        {
                            IPersistent obj = (IPersistent)e.oref.Target;
                            if (obj == null && e.dirty > 0) 
                            { 
                                goto waitFinalization;
                            }
                            return obj;
                        }
                    }
                    return null;
                }
            waitFinalization:
                GC.WaitForPendingFinalizers();
            }
        }
		

        internal void  rehash()
        {
            int oldCapacity = table.Length;
            Entry[] oldMap = table;
            int i;
            for (i = oldCapacity; --i >= 0; )
            {
                for (Entry prev = null, e = oldMap[i]; e != null; e = e.next)
                {
                    if (!e.oref.IsAlive && e.dirty == 0)
                    {
                        count -= 1;
                        if (prev == null)
                        {
                            oldMap[i] = e.next;
                        }
                        else
                        {
                            prev.next = e.next;
                        }
                    }
                    else
                    {
                        prev = e;
                    }
                }
            }
			
            if ((uint)count <= ((uint)threshold >> 1))
            {
                return ;
            }
            int newCapacity = oldCapacity * 2 + 1;
            Entry[] newMap = new Entry[newCapacity];
			
            threshold = (int) (newCapacity * loadFactor);
            table = newMap;
			
            for (i = oldCapacity; --i >= 0; )
            {
                for (Entry old = oldMap[i]; old != null; )
                {
                    Entry e = old;
                    old = old.next;
					
                    int index = (e.oid & 0x7FFFFFFF) % newCapacity;
                    e.next = newMap[index];
                    newMap[index] = e;
                }
            }
        }
		
        public void flush() 
        {
            while (true) 
            {
                lock (this) 
                { 
                    for (int i = 0; i < table.Length; i++) 
                    { 
                        for (Entry e = table[i]; e != null; e = e.next) 
                        { 
                            IPersistent obj = (IPersistent)e.oref.Target;
                            if (obj != null) 
                            { 
                                if (obj.IsModified()) 
                                { 
                                    obj.Store();
                                }
                            } 
                            else if (e.dirty != 0) 
                            { 
                                goto waitFinalization;
                            }
                        }
                    }
                    return;
                }
            waitFinalization:
                GC.WaitForPendingFinalizers();
            }
        }

        public void invalidate() 
        {
            while (true) 
            {
                lock (this) 
                { 
                    for (int i = 0; i < table.Length; i++) 
                    { 
                        for (Entry e = table[i]; e != null; e = e.next) 
                        { 
                            IPersistent obj = (IPersistent)e.oref.Target;
                            if (obj != null) 
                            { 
                                if (obj.IsModified()) 
                                { 
                                    e.dirty = 0;
                                    obj.Invalidate();
                                }
                            } 
                            else if (e.dirty != 0) 
                            { 
                                goto waitFinalization;
                            }
                        }
                    }
                    return;
                }
            waitFinalization:
                GC.WaitForPendingFinalizers();
            }
        }

        public void setDirty(int oid) 
        {
            lock (this) 
            { 
                Entry[] tab = table;
                int index = (oid & 0x7FFFFFFF) % tab.Length;
                for (Entry e = tab[index] ; e != null ; e = e.next) 
                {
                    if (e.oid == oid) 
                    {
                        e.dirty += 1;
                        return;
                    }
                }
            }
        }

        public void clearDirty(int oid) 
        {
            lock (this) 
            { 
                Entry[] tab = table;
                int index = (oid & 0x7FFFFFFF) % tab.Length;
                for (Entry e = tab[index] ; e != null ; e = e.next) 
                {
                    if (e.oid == oid) 
                    {
                        if (e.dirty > 0) 
                        { 
                            e.dirty -= 1;
                        }
                        return;
                    }
                }
            }
        }

        public int size()
        {
            return count;
        }

	
        internal class Entry
        {
            internal Entry next;
            internal WeakReference oref;
            internal int oid;
            internal int dirty;
		
            internal Entry(int oid, WeakReference oref, Entry chain)
            {
                next = chain;
                this.oid = oid;
                this.oref = oref;
            }
        }
    }
}


