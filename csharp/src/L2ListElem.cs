namespace NachoDB
{
    using System;

    /// <summary>
    /// Double linked list element.
    /// </summary>
#if USE_GENERICS
    public class L2ListElem<T> : Persistent where T:L2ListElem<T>
    { 
        internal T next;
        internal T prev;
#else
    public class L2ListElem : PersistentResource 
    { 
        internal L2ListElem next;
        internal L2ListElem prev;
#endif    
        /// <summary>
        /// Get next list element. 
        /// Been call for the last list element, this method will return first element of the list 
        /// or list header
        /// </summary>
#if USE_GENERICS
        public T Next
#else
        public L2ListElem Next
#endif
        { 
            get 
            {
                return next;
            }
        }

        /// <summary>
        /// Get previous list element. 
        /// Been call for the first list element, this method will return last element of the list 
        /// or list header
        /// </summary>
#if USE_GENERICS
        public T Prev 
#else
        public L2ListElem Prev 
#endif
        { 
            get 
            {
                return prev;
            }
        }

        /// <summary>
        /// Make list empty. 
        /// This method should be applied to list header. 
        /// </summary>
        public void Prune() 
        { 
            Modify();
            next = prev = null;
        }
    }
}