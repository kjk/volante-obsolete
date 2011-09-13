namespace Volante
{
    using System;

    /// <summary>
    /// Double linked list element.
    /// </summary>
    public class L2ListElem<T> : Persistent where T : L2ListElem<T>
    {
        internal T next;
        internal T prev;
        /// <summary>
        /// Get next list element. 
        /// Been called for the last list element, this method will return first element of the list 
        /// or list header
        /// </summary>
        public T Next
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
        public T Prev
        {
            get
            {
                return prev;
            }
        }
    }
}