namespace Volante.Impl
{
    using System;
    using Volante;

    public class StrongHashTable : OidHashTable
    {
        internal Entry[] table;
        internal const float loadFactor = 0.75f;
        internal int count;
        internal int threshold;

        public StrongHashTable(int initialCapacity)
        {
            threshold = (int)(initialCapacity * loadFactor);
            if (initialCapacity != 0)
                table = new Entry[initialCapacity];
        }

        public bool Remove(int oid)
        {
            lock (this)
            {
                Entry[] tab = table;
                int index = (oid & 0x7FFFFFFF) % tab.Length;
                for (Entry e = tab[index], prev = null; e != null; prev = e, e = e.next)
                {
                    if (e.oid == oid)
                    {
                        e.oref = null;
                        count -= 1;
                        if (prev != null)
                            prev.next = e.next;
                        else
                            tab[index] = e.next;
                        return true;
                    }
                }
                return false;
            }
        }

        public void Put(int oid, IPersistent obj)
        {
            lock (this)
            {
                Entry[] tab = table;
                int index = (oid & 0x7FFFFFFF) % tab.Length;
                for (Entry e = tab[index]; e != null; e = e.next)
                {
                    if (e.oid == oid)
                    {
                        e.oref = obj;
                        return;
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
                tab[index] = new Entry(oid, obj, tab[index]);
                count++;
            }
        }

        public IPersistent Get(int oid)
        {
            lock (this)
            {
                Entry[] tab = table;
                int index = (oid & 0x7FFFFFFF) % tab.Length;
                for (Entry e = tab[index]; e != null; e = e.next)
                {
                    if (e.oid == oid)
                        return e.oref;
                }
                return null;
            }
        }

        public void Flush()
        {
            lock (this)
            {
                for (int i = 0; i < table.Length; i++)
                {
                    for (Entry e = table[i]; e != null; e = e.next)
                    {
                        if (e.oref.IsModified())
                            e.oref.Store();
                    }
                }
            }
        }

        public void Invalidate()
        {
            lock (this)
            {
                for (int i = 0; i < table.Length; i++)
                {
                    for (Entry e = table[i]; e != null; e = e.next)
                    {
                        if (e.oref.IsModified())
                            e.oref.Invalidate();
                    }
                    table[i] = null;
                }
                count = 0;
            }
        }

        internal void rehash()
        {
            int oldCapacity = table.Length;
            Entry[] oldMap = table;
            int i;

            int newCapacity = oldCapacity * 2 + 1;
            Entry[] newMap = new Entry[newCapacity];

            threshold = (int)(newCapacity * loadFactor);
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

        public void SetDirty(int oid)
        {
        }

        public void ClearDirty(int oid)
        {
        }

        internal class Entry
        {
            internal Entry next;
            internal IPersistent oref;
            internal int oid;

            internal Entry(int oid, IPersistent oref, Entry chain)
            {
                next = chain;
                this.oid = oid;
                this.oref = oref;
            }
        }
    }
}