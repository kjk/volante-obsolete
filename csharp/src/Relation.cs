namespace Perst
{
    using System;
    using System.Collections;
	
    /// <summary> Class representing relation between owner and members
    /// </summary>
    public abstract class Relation:Persistent, Link
    {
        public abstract int Size();

        public abstract int Length 
        {
            get;
            set;
        }

        public abstract IPersistent this[int i] 
        {
            get;
            set;
        }
		
        public abstract IPersistent Get(int i);
		
        public abstract IPersistent GetRaw(int i);
		
        public abstract void  Set(int i, IPersistent obj);
		
        public abstract void  Remove(int i);

        public abstract void  Insert(int i, IPersistent obj);
		
        public abstract void  Add(IPersistent obj);
		
        public abstract void  AddAll(IPersistent[] arr);
		
        public abstract void  AddAll(IPersistent[] arr, int from, int length);
		
        public abstract void  AddAll(Link anotherLink);
		
        public abstract IPersistent[] ToArray();

        public abstract IPersistent[] ToRawArray();

        public abstract Array ToArray(Type elemType);

        public abstract bool  Contains(IPersistent obj);
		
        public abstract bool  ContainsElement(int i, IPersistent obj);

        public abstract int   IndexOf(IPersistent obj);
		
        public abstract void  Clear();

        public abstract void  Pin();

        public abstract void  Unpin();
 
        public abstract IEnumerator GetEnumerator();

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
                Store();
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
		
        internal Relation() {}

        private IPersistent owner;
    }
}