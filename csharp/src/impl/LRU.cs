namespace Volante.Impl
{
    using System;
    using Volante;

    class LRU
    {
        internal LRU next;
        internal LRU prev;

        internal LRU()
        {
            next = prev = this;
        }

        internal void unlink()
        {
            next.prev = prev;
            prev.next = next;
        }

        internal void link(LRU node)
        {
            node.next = next;
            node.prev = this;
            next.prev = node;
            next = node;
        }
    }
}