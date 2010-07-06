using System;
#if USE_GENERICS
using System.Collections.Generic;
#else
using System.Collections;
#endif
using Perst;


namespace Perst.Impl
{
	
#if USE_GENERICS
    public class RelationImpl<M,O>:Relation<M,O> where M:class,IPersistent where O:class,IPersistent
#else
    public class RelationImpl:Relation
#endif
    {
        public override int Count 
        { 
            get 
            {
                return link.Count;
            }
        }

#if USE_GENERICS
        public override void CopyTo(M[] dst, int i) 
#else
        public override void CopyTo(Array dst, int i) 
#endif
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

#if USE_GENERICS
        public override M this[int i] 
#else
        public override IPersistent this[int i] 
#endif
        {
            get 
            {
                return link.Get(i);
            }
            set 
            {
                link.Set(i, value);
                Modify();
            }
        }

        public override int Size()
        {
            return link.Length;
        }
		
#if USE_GENERICS
        public override M Get(int i)
#else
        public override IPersistent Get(int i)
#endif
        {
            return link.Get(i);
        }
		
        public override IPersistent GetRaw(int i)
        {
            return link.GetRaw(i);
        }
		
#if USE_GENERICS
        public override void Set(int i, M obj)
#else
        public override void Set(int i, IPersistent obj)
#endif
        {
            link.Set(i, obj);
            Modify();
        }
		
#if USE_GENERICS
        public override bool Remove(M obj) 
#else
        public override bool Remove(IPersistent obj) 
#endif
        {
            if (link.Remove(obj))
            { 
                Modify();
                return true;
            }
            return false;
        }

        public override void Remove(int i)
        {
            link.Remove(i);
            Modify();
        }
		
#if USE_GENERICS
        public override void Insert(int i, M obj)
#else
        public override void Insert(int i, IPersistent obj)
#endif
        {
            link.Insert(i, obj);
            Modify();
        }
		
#if USE_GENERICS
        public override void Add(M obj)
#else
        public override void Add(IPersistent obj)
#endif
        {
            link.Add(obj);
            Modify();
        }
		
#if USE_GENERICS
        public override void AddAll(M[] arr)
#else
        public override void AddAll(IPersistent[] arr)
#endif
        {
            link.AddAll(arr);
            Modify();
        }
		
#if USE_GENERICS
        public override void AddAll(M[] arr, int from, int length)
#else
        public override void AddAll(IPersistent[] arr, int from, int length)
#endif
        {
            link.AddAll(arr, from, length);
            Modify();
        }
		
#if USE_GENERICS
        public override void AddAll(Link<M> anotherLink)
#else
        public override void AddAll(Link anotherLink)
#endif
        {
            link.AddAll(anotherLink);
            Modify();
        }
		
#if USE_GENERICS
        public override M[] ToArray()
#else
        public override IPersistent[] ToArray()
#endif
        {
            return link.ToArray();
        }
		
        public override Array ToRawArray()
        {
            return link.ToRawArray();
        }
		
        public override Array ToArray(Type elemType)
        {
            return link.ToArray(elemType);
        }
		
#if USE_GENERICS
        public override bool Contains(M obj)
#else
        public override bool Contains(IPersistent obj)
#endif
        {
            return link.Contains(obj);
        }
		
#if USE_GENERICS
        public override bool ContainsElement(int i, M obj)
#else
        public override bool ContainsElement(int i, IPersistent obj)
#endif
        {
            return link.ContainsElement(i, obj);
        }

#if USE_GENERICS
        public override int IndexOf(M obj)
#else
        public override int IndexOf(IPersistent obj)
#endif
        {
            return link.IndexOf(obj);
        }
		
#if USE_GENERICS
        public override IEnumerator<M> GetEnumerator() 
#else
        public override IEnumerator GetEnumerator() 
#endif
        {
            return link.GetEnumerator();

        }

        public override void Clear() 
        {
            link.Clear();
            Modify();
        }
		
        public override void Unpin()
        {
            link.Unpin();
        }

        public override void Pin()
        {
            link.Pin();
        }

#if USE_GENERICS
        internal RelationImpl(O owner):base(owner)
        {
            link = new LinkImpl<M>(8);
        }
#else
        internal RelationImpl(IPersistent owner):base(owner)
        {
            link = new LinkImpl(8);
        }
#endif
		
        internal RelationImpl() {}

#if USE_GENERICS
        internal Link<M> link;
#else
        internal Link    link;
#endif
    }
}