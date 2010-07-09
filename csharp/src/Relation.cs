namespace NachoDB
{
    using System;
#if USE_GENERICS
    using System.Collections.Generic;
#else
    using System.Collections;
#endif

    /// <summary> Class representing relation between owner and members
    /// </summary>
#if USE_GENERICS
    public abstract class Relation<M,O> : PersistentCollection<M>, Link<M> where M:class,IPersistent where O:class,IPersistent
#else
    public abstract class Relation : PersistentCollection, Link
#endif
    {
        public abstract int Size();

        public abstract int Length 
        {
            get;
            set;
        }

#if USE_GENERICS
        public abstract M this[int i] 
#else
        public abstract IPersistent this[int i] 
#endif
        {
            get;
            set;
        }
		
#if USE_GENERICS
        public abstract M Get(int i);
#else
        public abstract IPersistent Get(int i);
#endif
		
        public abstract IPersistent GetRaw(int i);
		
#if USE_GENERICS
        public abstract void  Set(int i, M obj);
#else
        public abstract void  Set(int i, IPersistent obj);
#endif
		
#if !USE_GENERICS
        public abstract bool  Remove(IPersistent obj);
#endif

#if USE_GENERICS
        public abstract void  RemoveAt(int i);
#endif
        public abstract void  Remove(int i);

#if USE_GENERICS
        public abstract void  Insert(int i, M obj);
#else
        public abstract void  Insert(int i, IPersistent obj);
#endif
		
#if !USE_GENERICS
        public abstract void  Add(IPersistent obj);
#endif
		
#if USE_GENERICS
        public abstract void  AddAll(M[] arr);
#else
        public abstract void  AddAll(IPersistent[] arr);
#endif
		
#if USE_GENERICS
        public abstract void  AddAll(M[] arr, int from, int length);
#else
        public abstract void  AddAll(IPersistent[] arr, int from, int length);
#endif
		
#if USE_GENERICS
        public abstract void  AddAll(Link<M> anotherLink);
#else
        public abstract void  AddAll(Link anotherLink);
#endif		
      
#if USE_GENERICS
        public abstract M[] ToArray();
#else
        public abstract IPersistent[] ToArray();
#endif

        public abstract Array ToRawArray();

        public abstract Array ToArray(Type elemType);

#if !USE_GENERICS
        public abstract bool  Contains(IPersistent obj);
#endif
		
#if USE_GENERICS
        public abstract bool  ContainsElement(int i, M obj);
#else
        public abstract bool  ContainsElement(int i, IPersistent obj);
#endif

#if USE_GENERICS
        public abstract int   IndexOf(M obj);
#else
        public abstract int   IndexOf(IPersistent obj);
#endif
		
#if !USE_GENERICS
        public abstract void  Clear();
#endif

        public abstract void  Pin();

        public abstract void  Unpin();
 
#if USE_GENERICS
        public virtual O Owner
#else
        public virtual IPersistent Owner
#endif
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
                Modify();
            }			
        }

        /// <summary> Relation constructor. Creates empty relation with specified owner and no members. 
        /// Members can be added to the relation later.
        /// </summary>
        /// <param name="owner">owner of the relation
        /// 
        /// </param>		
#if USE_GENERICS
        public Relation(O owner)
#else
        public Relation(IPersistent owner)
#endif
        {
            this.owner = owner;
        }
		
        internal Relation() {}

        public void SetOwner(IPersistent obj)
        { 
#if USE_GENERICS
             owner = (O)obj;
#else
             owner = obj;
#endif
        }

#if USE_GENERICS
        private O owner;
#else
        private IPersistent owner;
#endif
    }
}