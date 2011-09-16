using System;
using System.Collections;
using System.Collections.Generic;
using Volante;


namespace Volante.Impl
{
    public class RelationImpl<M, O> : Relation<M, O>
        where M : class,IPersistent
        where O : class,IPersistent
    {
        public override int Count
        {
            get
            {
                return link.Count;
            }
        }

        public override void CopyTo(M[] dst, int i)
        {
            link.CopyTo(dst, i);
        }

        public override int Length
        {
            get
            {
                return link.Length;
            }
            set
            {
                link.Length = value;
            }
        }

        public override M this[int i]
        {
            get
            {
                return link.Get(i);
            }
            set
            {
                link.Set(i, value);
            }
        }

        public override int Size()
        {
            return link.Length;
        }

        public override M Get(int i)
        {
            return link.Get(i);
        }

        public override IPersistent GetRaw(int i)
        {
            return link.GetRaw(i);
        }

        public override void Set(int i, M obj)
        {
            link.Set(i, obj);
        }

        public override bool Remove(M obj)
        {
            return link.Remove(obj);
        }

        public override void RemoveAt(int i)
        {
            link.RemoveAt(i);
        }

        public override void Insert(int i, M obj)
        {
            link.Insert(i, obj);
        }

        public override void Add(M obj)
        {
            link.Add(obj);
        }

        public override void AddAll(M[] arr)
        {
            link.AddAll(arr);
        }

        public override void AddAll(M[] arr, int from, int length)
        {
            link.AddAll(arr, from, length);
        }

        public override void AddAll(ILink<M> anotherLink)
        {
            link.AddAll(anotherLink);
        }

        public override M[] ToArray()
        {
            return link.ToArray();
        }

        public override Array ToRawArray()
        {
            return link.ToRawArray();
        }

        public override bool Contains(M obj)
        {
            return link.Contains(obj);
        }

        public override bool ContainsElement(int i, M obj)
        {
            return link.ContainsElement(i, obj);
        }

        public override int IndexOf(M obj)
        {
            return link.IndexOf(obj);
        }

        public override IEnumerator<M> GetEnumerator()
        {
            return link.GetEnumerator();
        }

        public override void Clear()
        {
            link.Clear();
        }

        public override void Unpin()
        {
            link.Unpin();
        }

        public override void Pin()
        {
            link.Pin();
        }

        internal RelationImpl(O owner)
            : base(owner)
        {
            link = new LinkImpl<M>(8);
        }

        internal RelationImpl() { }

        internal ILink<M> link;
    }
}
