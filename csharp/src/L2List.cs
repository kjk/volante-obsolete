namespace Perst
{
    using System;
    using System.Collections;

    /// <summary>
    /// Double linked list.
    /// </summary>
    public class L2List : L2ListElem, ICollection, IEnumerable
    { 
        private int nElems;
        private int updateCounter;

        /// <summary>
        /// Get list head element
        /// </summary>
        /// <returns>list head element or null if list is empty
        /// </returns>>
        public L2ListElem Head 
        {
            get 
            {
                lock (this) 
                { 
                    return next != this ? next : null;
                }
            }
        }

        /// <summary>
        /// Get list tail element
        /// </summary>
        /// <returns>list tail element or null if list is empty
        /// </returns>>
        public L2ListElem Tail
        { 
            get 
            { 
                lock (this) 
                { 
                    return prev != this ? prev : null;
                }
            }
        }

        /// <summary>
        /// Make list empty. 
        /// </summary>
        public void Clear() 
        { 
            lock (this) 
            {
                Modify();
                next = prev = null;
                nElems = 0;
                updateCounter += 1;
            }
        }

        /// <summary>
        /// Insert element at the beginning of the list
        /// </summary>
        public void Prepend(L2ListElem elem) 
        { 
            lock (this) 
            { 
                Modify();
                next.Modify();
                elem.Modify();
                elem.next = next;
                elem.prev = this;
                next.prev = elem;
                next = elem;
                nElems += 1;
                updateCounter += 1;
            }
        }

        /// <summary>
        /// Insert element at the end of the list
        /// </summary>
        public void Append(L2ListElem elem) 
        { 
            lock (this) 
            { 
                Modify();
                prev.Modify();
                elem.Modify(); 
                elem.next = this;
                elem.prev = prev;
                prev.next = elem;
                prev = elem;
                nElems += 1;
                updateCounter += 1;
            }
        }

        /// <summary>
        /// Remove element from the list
        /// </summary>
        public void Remove(L2ListElem elem) 
        { 
            lock (this) 
            { 
                Modify();
                elem.prev.Modify();
                elem.next.Modify();
                elem.next.prev = elem.prev;
                elem.prev.next = elem.next;
                nElems -= 1;
                updateCounter += 1;
            }
        }

        /// <summary>
        /// Add element to the list
        /// </summary>
        public void Add(L2ListElem elem) 
        { 
            Append(elem);
        }
        public int Count 
        { 
            get 
            {
                return nElems;
            }
        }

        public bool IsSynchronized 
        {
            get 
            {
                return true;
            }
        }

        public object SyncRoot 
        {
            get 
            {
                return this;
            }
        }

        public void CopyTo(Array dst, int i) 
        {
            lock (this) 
            { 
                foreach (object o in this) 
                { 
                    dst.SetValue(o, i++);
                }
            }
        }

        
        class L2ListEnumerator : IEnumerator
        {
            private L2ListElem curr;
            private int        counter;
            private L2List     list;

            internal L2ListEnumerator(L2List list) 
            { 
                this.list = list;
                Reset();
            }

            public void Reset() 
            { 
                curr = list;
                counter = list.updateCounter;
            }

            public object Current
            { 
                get 
                { 
                    if (curr == list || counter != list.updateCounter) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return curr;
                }
            }

            public bool MoveNext() 
            { 
                if (counter != list.updateCounter) 
                { 
                    throw new InvalidOperationException();
                }
                if (curr.next == list) 
                { 
                    return false;
                }
                curr = curr.next;
                return true;
            }
        }
            
        
        public IEnumerator GetEnumerator() 
        { 
            return new L2ListEnumerator(this);
        }
    }
}
