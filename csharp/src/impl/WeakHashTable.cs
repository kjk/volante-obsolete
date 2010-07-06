namespace Perst.Impl
{
    using System;
    using Perst;
	
    public class WeakHashTable
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
            lock(this)
            {
                Entry[] tab = table;
                int index = (oid & 0x7FFFFFFF) % tab.Length;
                for (Entry e = tab[index]; e != null; e = e.next)
                {
                    if (e.oid == oid)
                    {
                        return (IPersistent) e.oref.Target;
                    }
                }
                return null;
            }
        }
		
        public void clear() 
        { 
            lock(this)
            {
                for (int i = 0; i < table.Length; i++) 
                { 
                    table[i] = null;
                }
                count = 0;
            }
        }

        public void flush() 
        {
            for (int i = 0; i < table.Length; i++) 
            { 
                for (Entry e = table[i]; e != null; e = e.next) 
                { 
                    IPersistent obj = (IPersistent)e.oref.Target;
                    if (obj != null && obj.isModified()) 
                    { 
                        obj.store();
                    }
                }   
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
                    if (!e.oref.IsAlive)
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
		
        public virtual int size()
        {
            return count;
        }
    }
	
    class Entry
    {
        internal Entry next;
        internal WeakReference oref;
        internal int oid;
		
        internal Entry(int oid, WeakReference oref, Entry chain)
        {
            next = chain;
            this.oid = oid;
            this.oref = oref;
        }
    }
}