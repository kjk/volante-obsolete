namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    /// <summary>
    /// Double linked list.
    /// </summary>
    public class L2List<T> : PersistentCollection<T> where T : L2ListElem<T>
    {
        T head;
        T tail;

        private int nElems;
        private int updateCounter;

        /// <summary>
        /// Get list head element
        /// </summary>
        /// <returns>list head element or null if list is empty
        /// </returns>>
        public T Head
        {
            get
            {
                return head;
            }
        }

        /// <summary>
        /// Get list tail element
        /// </summary>
        /// <returns>list tail element or null if list is empty
        /// </returns>
        public T Tail
        {
            get
            {
                return tail;
            }
        }

        public override bool Contains(T obj)
        {
            foreach (T o in this)
            {
                if (o == obj)
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Make list empty. 
        /// </summary>
        public override void Clear()
        {
            lock (this)
            {
                Modify();
                head = tail = null;
                nElems = 0;
                updateCounter += 1;
            }
        }

        /// <summary>
        /// Insert element at the beginning of the list
        /// </summary>
        public void Prepend(T elem)
        {
            lock (this)
            {
                Modify();
                elem.Modify();
                elem.next = head;
                elem.prev = null;
                if (head != null)
                {
                    head.Modify();
                    head.prev = elem;
                }
                else
                    tail = elem;

                head = elem;
                nElems += 1;
                updateCounter += 1;
            }
        }

        /// <summary>
        /// Insert element at the end of the list
        /// </summary>
        public void Append(T elem)
        {
            lock (this)
            {
                Modify();
                elem.Modify();
                elem.next = null;
                elem.prev = tail;
                if (tail != null)
                {
                    tail.Modify();
                    tail.next = elem;
                }
                else
                    tail = elem;

                tail = elem;
                if (head == null)
                    head = elem;
                nElems += 1;
                updateCounter += 1;
            }
        }

        /// <summary>
        /// Remove element from the list
        /// </summary>
        public override bool Remove(T elem)
        {
            lock (this)
            {
                Modify();
                if (elem.prev != null)
                {
                    elem.prev.Modify();
                    elem.prev.next = elem.next;
                    elem.prev = null;
                }
                else
                    head = head.next;

                if (elem.next != null)
                {
                    elem.next.Modify();
                    elem.next.prev = elem.prev;
                    elem.next = null;
                }
                else
                    tail = tail.prev;

                nElems -= 1;
                updateCounter += 1;
                return true;
            }
        }

        /// <summary>
        /// Add element to the list
        /// </summary>
        public override void Add(T elem)
        {
            Append(elem);
        }

        public override int Count
        {
            get
            {
                return nElems;
            }
        }

        class L2ListEnumerator : IEnumerator<T>
        {
            private T curr;
            private int counter;
            private L2List<T> list;
            private bool head;

            internal L2ListEnumerator(L2List<T> list)
            {
                this.list = list;
                Reset();
            }

            public void Reset()
            {
                curr = null;
                counter = list.updateCounter;
                head = true;
            }

            public T Current
            {
                get
                {
                    if (curr == null || counter != list.updateCounter)
                        throw new InvalidOperationException();
                    return curr;
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return Current;
                }
            }

            public void Dispose() { }

            public bool MoveNext()
            {
                if (counter != list.updateCounter)
                    throw new InvalidOperationException();

                if (head)
                {
                    curr = list.head;
                    head = false;
                }
                else if (curr != null)
                    curr = curr.next;

                return curr != null;
            }
        }

        public override IEnumerator<T> GetEnumerator()
        {
            return new L2ListEnumerator(this);
        }
    }
}
