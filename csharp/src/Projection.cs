namespace Perst
{
    using System;
    using System.Collections;
    using System.Reflection;

    /// <summary>
    /// Class use to project selected objects using relation field. 
    /// For all selected objects (specified by array ort iterator), 
    /// value of specified field (of IPersistent, array of IPersistent, Link or Relation type)
    /// is inspected and all referenced object for projection (duplicate values are eliminated)
    /// </summary>
    public class Projection : IEnumerable
    { 
        /// <summary>
        /// Constructor of projection specified by class and field name of projected objects
        /// </summary>
        /// <param name="type">base class for selected objects</param>
        /// <param name="fieldName">field name used to perform projection</param>
        public Projection(Type type, String fieldName) 
        { 
            SetProjectionField(type, fieldName);
        }

        /// <summary>
        /// Default constructor of projection. This constructor should be used
        /// only when you are going to derive your class from Projection and redefine
        /// map method in it or sepcify type and fieldName later using setProjectionField
        /// method
        /// </summary>
        public Projection() {}

        /// <summary>
        /// Specify class of the projected objects and projection field name
        /// </summary>
        /// <param name="type">base class for selected objects</param>
        /// <param name="fieldName">field name used to perform projection</param>
        public void SetProjectionField(Type type, String fieldName) 
        { 
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
        public void Project(IPersistent[] selection) 
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
        public void Project(IPersistent obj) 
        { 
            Map(obj);
        } 

        /// <summary>
        /// Project specified selection
        /// </summary>
        /// <param name="selection">enumerator specifying selceted objects</param>
        public void Project(IEnumerator selection) 
        { 
            while (selection.MoveNext()) 
            { 
                Map((IPersistent)selection.Current);
            }
        } 

        /// <summary>
        /// Project specified selection
        /// </summary>
        /// <param name="selection">enumerator specifying selceted objects</param>
        public void Project(IEnumerable selection) 
        { 
            foreach (IPersistent obj in selection) 
            { 
                Map(obj);
            }
        } 

        /// <summary>
        /// Join this projection with another projection.
        /// Result of this join is set of objects present in both projections.
        /// </summary>
        /// <param name="prj">joined projection</param>
        public void Join(Projection prj) 
        { 
            Hashtable join = new Hashtable();
            foreach (IPersistent p in prj.hash.Keys) 
            {
                if (hash.Contains(p)) 
                { 
                    join[p] = p;
                }
            }
            hash = join;
        }
 
        /// <summary>
        /// Get result of preceding project and join operations
        /// </summary>
        /// <returns>array of objects</returns>
        public IPersistent[] ToArray() 
        { 
            IPersistent[] arr = new IPersistent[hash.Count];
            hash.Keys.CopyTo(arr, 0);
            return arr;
        }

 
        /// <summary>
        /// Get result of preceding project and join operations
        /// </summary>
        /// <param name="elemType">type of result array element</param>
        /// <returns>array of objects</returns>
        public IPersistent[] ToArray(Type elemType) 
        { 
            IPersistent[] arr = (IPersistent[])Array.CreateInstance(elemType, hash.Count);
            hash.Keys.CopyTo(arr, 0);
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
        public IEnumerator GetEnumerator() 
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
        protected void Add(IPersistent obj) 
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
    
        private Hashtable  hash = new Hashtable();
        private FieldInfo  field;
    }
}
