namespace NachoDB
{
    using System;
#if USE_GENERICS
    using System.Collections.Generic;
#else
    using System.Collections;
#endif
    using System.Reflection;

    /// <summary>
    /// Class use to project selected objects using relation field. 
    /// For all selected objects (specified by array ort iterator), 
    /// value of specified field (of IPersistent, array of IPersistent, Link or Relation type)
    /// is inspected and all referenced object for projection (duplicate values are eliminated)
    /// </summary>
#if USE_GENERICS
    public class Projection<From,To> : ICollection<To> where From:class,IPersistent where To:class,IPersistent
#else
    public class Projection : ICollection
#endif
    { 
#if USE_GENERICS
        /// <summary>
        /// Constructor of projection specified by field name of projected objects
        /// </summary>
        /// <param name="fieldName">field name used to perform projection</param>
        public Projection(String fieldName) 
        { 
            SetProjectionField(fieldName);
        }
#else
        /// <summary>
        /// Constructor of projection specified by class and field name of projected objects
        /// </summary>
        /// <param name="type">base class for selected objects</param>
        /// <param name="fieldName">field name used to perform projection</param>
        public Projection(Type type, string fieldName) 
        { 
            SetProjectionField(type, fieldName);
        }
#endif

        /// <summary>
        /// Default constructor of projection. This constructor should be used
        /// only when you are going to derive your class from Projection and redefine
        /// map method in it or sepcify type and fieldName later using setProjectionField
        /// method
        /// </summary>
        public Projection() {}

        public int Count 
        { 
            get 
            {
                return hash.Count;
            }
        }

        public bool IsSynchronized 
        {
            get 
            {
                return false;
            }
        }

        public object SyncRoot 
        {
            get 
            {
                return null;
            }
        }

#if USE_GENERICS
        public void CopyTo(To[] dst, int i) 
#else
        public void CopyTo(Array dst, int i) 
#endif
        {
            foreach (object o in this) 
            { 
                dst.SetValue(o, i++);
            }
        }

#if USE_GENERICS
        /// <summary>
        /// Specify projection field name
        /// </summary>
        /// <param name="fieldName">field name used to perform projection</param>
        public void SetProjectionField(string fieldName) 
        { 
            Type type = typeof(From);
#else
        /// <summary>
        /// Specify class of the projected objects and projection field name
        /// </summary>
        /// <param name="type">base class for selected objects</param>
        /// <param name="fieldName">field name used to perform projection</param>
        public void SetProjectionField(Type type, string fieldName) 
        { 
#endif
            field = type.GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
            if (field == null) 
            { 
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
            }
        }

        /// <summary>
        /// Project specified selection
        /// </summary>
        /// <param name="selection">array with selected object</param>
#if USE_GENERICS  
        public void Project(From[] selection) 
#else
        public void Project(IPersistent[] selection) 
#endif
        { 
            for (int i = 0; i < selection.Length; i++) 
            { 
                Map(selection[i]);
            }
        } 

        /// <summary>
        /// Project specified object
        /// </summary>
        /// <param name="obj">selected object</param>
#if USE_GENERICS
        public void Project(From obj) 
#else
        public void Project(IPersistent obj) 
#endif
        { 
            Map(obj);
        } 

        /// <summary>
        /// Project specified selection
        /// </summary>
        /// <param name="selection">enumerator specifying selceted objects</param>
#if USE_GENERICS
        public void Project(IEnumerator<From> selection) 
        { 
            while (selection.MoveNext()) 
            { 
                Map(selection.Current);
            }
        } 
#else
        public void Project(IEnumerator selection) 
        { 
            while (selection.MoveNext()) 
            { 
                Map((IPersistent)selection.Current);
            }
        } 
#endif

        /// <summary>
        /// Project specified selection
        /// </summary>
        /// <param name="selection">enumerator specifying selceted objects</param>
#if USE_GENERICS
        public void Project(IEnumerable<From> selection) 
        { 
            foreach (From obj in selection) 
            { 
                Map(obj);
            }
        } 
#else
        public void Project(IEnumerable selection) 
        { 
            foreach (IPersistent obj in selection) 
            { 
                Map(obj);
            }
        } 
#endif

        /// <summary>
        /// Join this projection with another projection.
        /// Result of this join is set of objects present in both projections.
        /// </summary>
        /// <param name="prj">joined projection</param>
#if USE_GENERICS
        public void Join<X>(Projection<X,To> prj) where X:class,IPersistent
        { 
            Dictionary<To,To> join = new Dictionary<To,To>();
            foreach (To p in prj.hash.Keys) 
            {
                if (hash.ContainsKey(p)) 
                { 
                    join[p] = p;
                }
            }
            hash = join;
        }
#else
        public void Join(Projection prj) 
        { 
            Hashtable join = new Hashtable();
            foreach (IPersistent p in prj.hash.Keys) 
            {
                if (hash.ContainsKey(p)) 
                { 
                    join[p] = p;
                }
            }
            hash = join;
        }
#endif
 
        /// <summary>
        /// Get result of preceding project and join operations
        /// </summary>
        /// <returns>array of objects</returns>
#if USE_GENERICS
        public To[] ToArray() 
        { 
            To[] arr = new To[hash.Count];
            hash.Keys.CopyTo(arr, 0);
            return arr;
        }
#else
        public IPersistent[] ToArray() 
        { 
            IPersistent[] arr = new IPersistent[hash.Count];
            hash.Keys.CopyTo(arr, 0);
            return arr;
        }
#endif
 
        /// <summary>
        /// Get result of preceding project and join operations
        /// </summary>
        /// <param name="elemType">type of result array element</param>
        /// <returns>array of objects</returns>
        public Array ToArray(Type elemType) 
        { 
            Array arr = Array.CreateInstance(elemType, hash.Count);
#if USE_GENERICS
            hash.Keys.CopyTo((To[])arr, 0);
#else
            hash.Keys.CopyTo(arr, 0);
#endif
            return arr;
        }

        /// <summary>
        /// Get number of objets in the result 
        /// </summary>
        public int Length 
        { 
            get 
            {
                return hash.Count;
            }
        }

        /// <summary>
        /// Get enumerator for result of preceding project and join operations
        /// </summary>
        /// <returns>enumerator</returns>
#if USE_GENERICS
        public IEnumerator<To> GetEnumerator() 
#else
        public IEnumerator GetEnumerator() 
#endif
        { 
            return hash.Keys.GetEnumerator();
        }

        /// <summary>
        /// Reset projection - clear result of prceding project and join operations
        /// </summary>
        public void Reset() 
        { 
            hash.Clear();
        }

        /// <summary>
        /// Add object to the set
        /// </summary>
        /// <param name="obj">object to be added to the set</param>
#if USE_GENERICS
        public void Add(To obj) 
#else
        public void Add(IPersistent obj) 
#endif
        { 
            if (obj != null) 
            {
                hash[obj] = obj;
            }
        }


        /// <summary>
        /// Get related objects for the object obj. 
        /// It is possible to redifine this method in derived classes 
        /// to provide application specific mapping
        /// </summary>
        /// <param name="obj">object from the selection</param>
#if USE_GENERICS
        protected void Map(From obj) 
        {   
            if (field == null) 
            { 
                Add((To)(object)obj);
            } 
            else 
            { 
                object o = field.GetValue(obj);
                if (o is Link<To>) 
                { 
                    To[] arr = ((Link<To>)o).ToArray();
                    for (int i = 0; i < arr.Length; i++) 
                    { 
                        Add(arr[i]);
                    }
                } 
                else if (o is To[]) 
                { 
                    To[] arr = (To[])o;
                    for (int i = 0; i < arr.Length; i++) 
                    { 
                        Add(arr[i]);                            
                    }
                } 
                else 
                { 
                    Add((To)o);
                }
            }
        } 
#else
        protected void Map(IPersistent obj) 
        {   
            if (field == null) 
            { 
                Add(obj);
            } 
            else 
            { 
                object o = field.GetValue(obj);
                if (o is Link) 
                { 
                    IPersistent[] arr = ((Link)o).ToArray();
                    for (int i = 0; i < arr.Length; i++) 
                    { 
                        Add(arr[i]);
                    }
                } 
                else if (o is object[]) 
                { 
                    IPersistent[] arr = (IPersistent[])o;
                    for (int i = 0; i < arr.Length; i++) 
                    { 
                        Add(arr[i]);
                            
                    }
                } 
                else 
                { 
                    Add((IPersistent)o);
                }
            }
        } 
#endif
    
#if USE_GENERICS
        public bool IsReadOnly
        {
            get
            {
                return false;
            }
        }

        public bool Contains(To obj) 
        { 
             return hash[obj] != null;
        }

        public bool Remove(To obj) 
        { 
             return hash.Remove(obj);
        }

        public void Clear() 
        { 
            hash.Clear();
        }
#endif


#if USE_GENERICS
        private Dictionary<To,To> hash = new Dictionary<To,To>();
#else
        private Hashtable  hash = new Hashtable();
#endif

        private FieldInfo  field;
    }
}
