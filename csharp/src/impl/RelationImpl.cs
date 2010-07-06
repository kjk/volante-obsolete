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
        }
		
        public override void Remove(int i)
        {
            link.Remove(i);
        }
		
        public override void Insert(int i, IPersistent obj)
        {
            link.Insert(i, obj);
        }
		
        public override void Add(IPersistent obj)
        {
            link.Add(obj);
        }
		
        public override void AddAll(IPersistent[] arr)
        {
            link.AddAll(arr);
        }
		
        public override void AddAll(IPersistent[] arr, int from, int length)
        {
            link.AddAll(arr, from, length);
        }
		
        public override void AddAll(Link anotherLink)
        {
            link.AddAll(anotherLink);
        }
		
        public override IPersistent[] ToArray()
        {
            return link.ToArray();
        }
		
        public override Array ToArray(Type elemType)
        {
            return link.ToArray(elemType);
        }
		
        public override bool Contains(IPersistent obj)
        {
            return link.Contains(obj);
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
        }
		
        internal RelationImpl(IPersistent owner):base(owner)
        {
            link = new LinkImpl(8);
        }
		
        internal LinkImpl link;
    }
}