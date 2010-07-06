namespace Perst.Impl
{
    using System;
    using Perst;
	
    public class RelationImpl:Relation
    {
        public override int size()
        {
            return link.size();
        }
		
        public override IPersistent get(int i)
        {
            return link.get(i);
        }
		
        public override IPersistent getRaw(int i)
        {
            return link.getRaw(i);
        }
		
        public override void  set(int i, IPersistent obj)
        {
            link.set(i, obj);
        }
		
        public override void  remove(int i)
        {
            link.remove(i);
        }
		
        public override void  insert(int i, IPersistent obj)
        {
            link.insert(i, obj);
        }
		
        public override void  add(IPersistent obj)
        {
            link.add(obj);
        }
		
        public override void  addAll(IPersistent[] arr)
        {
            link.addAll(arr);
        }
		
        public override void  addAll(IPersistent[] arr, int from, int length)
        {
            link.addAll(arr, from, length);
        }
		
        public override void  addAll(Link anotherLink)
        {
            link.addAll(anotherLink);
        }
		
        public override IPersistent[] toArray()
        {
            return link.toArray();
        }
		
        public override bool contains(IPersistent obj)
        {
            return link.contains(obj);
        }
		
        public override int indexOf(IPersistent obj)
        {
            return link.indexOf(obj);
        }
		
        public override void  clear()
        {
            link.clear();
        }
		
        internal RelationImpl(IPersistent owner):base(owner)
        {
            link = new LinkImpl(8);
        }
		
        internal LinkImpl link;
    }
}