namespace Perst
{
    using System;

    /// <summary>
    /// Double linked list element.
    /// </summary>
    public class L2ListElem : PersistentResource 
    { 
        internal L2ListElem next;
        internal L2ListElem prev;
    
        /// <summary>
        /// Get next list element. 
        /// Been call for the last list element, this method will return first element of the list 
        /// or list header
        /// </summary>
        public L2ListElem Next
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
        public L2ListElem Prev 
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

        /// <summary>
        /// Link specified element in the list after this element
        /// </summary>
        public void LinkAfter(L2ListElem elem) 
        { 
            Modify();
            next.Modify();
            elem.Modify();
            elem.next = next;
            elem.prev = this;
            next.prev = elem;
            next = elem;
        }

        /// <summary>
        /// Link specified element in the list before this element
        /// </summary>
        public void LinkBefore(L2ListElem elem) 
        { 
            Modify();
            prev.Modify();
            elem.Modify();
            elem.next = this;
            elem.prev = prev;
            prev.next = elem;
            prev = elem;
        }

        /// <summary>
        /// Remove element from the list
        /// </summary>
        public void Unlink() 
        { 
            next.Modify();
            prev.Modify();
            next.prev = prev;
            prev.next = next;
        }
    }
}