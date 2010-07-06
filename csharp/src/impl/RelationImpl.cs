namespace Perst.Impl
{
    using System;
    using System.Collections;
    using Perst;
	
    public class RelationImpl:Relation
    {
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

        public override IPersistent this[int i] 
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
		
        public override IPersistent Get(int i)
        {
            return link.Get(i);
        }
		
        public override IPersistent GetRaw(int i)
        {
            return link.GetRaw(i);
        }
		
        public override void Set(int i, IPersistent obj)
        {
            link.Set(i, obj);
            Modify();
        }
		
        public override void Remove(int i)
        {
            link.Remove(i);
            Modify();
        }
		
        public override void Insert(int i, IPersistent obj)
        {
            link.Insert(i, obj);
            Modify();
        }
		
        public override void Add(IPersistent obj)
        {
            link.Add(obj);
            Modify();
        }
		
        public override void AddAll(IPersistent[] arr)
        {
            link.AddAll(arr);
            Modify();
        }
		
        public override void AddAll(IPersistent[] arr, int from, int length)
        {
            link.AddAll(arr, from, length);
            Modify();
        }
		
        public override void AddAll(Link anotherLink)
        {
            link.AddAll(anotherLink);
            Modify();
        }
		
        public override IPersistent[] ToArray()
        {
            return link.ToArray();
        }
		
        public override IPersistent[] ToRawArray()
        {
            return link.ToRawArray();
        }
		
        public override Array ToArray(Type elemType)
        {
            return link.ToArray(elemType);
        }
		
        public override bool Contains(IPersistent obj)
        {
            return link.Contains(obj);
        }
		
        public override bool ContainsElement(int i, IPersistent obj)
        {
            return link.ContainsElement(i, obj);
        }

        public override int IndexOf(IPersistent obj)
        {
            return link.IndexOf(obj);
        }
		
        public override IEnumerator GetEnumerator() 
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

        internal RelationImpl(IPersistent owner):base(owner)
        {
            link = new LinkImpl(8);
        }
		
        internal RelationImpl() {}

        internal Link link;
    }
}