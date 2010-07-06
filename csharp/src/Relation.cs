namespace Perst
{
    using System;
	
    /// <summary> Class representing relation between owner and members
    /// </summary>
    public abstract class Relation:Persistent, Link
    {
        public abstract int size();
		
        public abstract IPersistent get(int i);
		
        public abstract IPersistent getRaw(int i);
		
        public abstract void  set(int i, IPersistent obj);
		
        public abstract void  remove(int i);

        public abstract void  insert(int i, IPersistent obj);
		
        public abstract void  add(IPersistent obj);
		
        public abstract void  addAll(IPersistent[] arr);
		
        public abstract void  addAll(IPersistent[] arr, int from, int length);
		
        public abstract void  addAll(Link anotherLink);
		
        public abstract IPersistent[] toArray();

        public abstract bool contains(IPersistent obj);
		
        public abstract int indexOf(IPersistent obj);
		
        public abstract void  clear();

        public virtual IPersistent Owner
        {
            /// <summary> Get relation owner
            /// </summary>
            /// <returns>owner of the relation
            /// 
            /// </returns>
            get
            {
                return owner;
            }
			
            /// <summary> Set relation owner
            /// </summary>
            /// <param name="owner">new owner of the relation
            /// 
            /// </param>
            set
            {
                this.owner = value;
                store();
            }
			
        }
        /// <summary> Relation constructor. Creates empty relation with specified owner and no members. 
        /// Members can be added to the relation later.
        /// </summary>
        /// <param name="owner">owner of the relation
        /// 
        /// </param>		
        public Relation(IPersistent owner)
        {
            this.owner = owner;
        }
		
        private IPersistent owner;
    }
}