namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Reflection;

    /// <summary>
    /// Class used to project selected objects using relation field. 
    /// For all selected objects (specified by array or iterator), 
    /// value of specified field (of IPersistent, array of IPersistent, Link or Relation type)
    /// is inspected and all referenced object for projection (duplicate values are eliminated)
    /// </summary>
    public class Projection<From, To> : ICollection<To>
        where From : class,IPersistent
        where To : class,IPersistent
    {
        /// <summary>
        /// Constructor of projection specified by field name of projected objects
        /// </summary>
        /// <param name="fieldName">field name used to perform projection</param>
        public Projection(String fieldName)
        {
            SetProjectionField(fieldName);
        }

        /// <summary>
        /// Default constructor of projection. This constructor should be used
        /// only when you are going to derive your class from Projection and redefine
        /// Map() method in it or sepcify type and fieldName later using SetProjectionField()
        /// method
        /// </summary>
        public Projection() { }

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

        public void CopyTo(To[] dst, int i)
        {
            foreach (object o in this)
            {
                dst.SetValue(o, i++);
            }
        }

        /// <summary>
        /// Specify projection field name
        /// </summary>
        /// <param name="fieldName">field name used to perform projection</param>
        public void SetProjectionField(string fieldName)
        {
            Type type = typeof(From);
            field = type.GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
            if (field == null)
                throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND);
        }

        /// <summary>
        /// Project specified selection
        /// </summary>
        /// <param name="selection">array with selected object</param>
        public void Project(From[] selection)
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
        public void Project(From obj)
        {
            Map(obj);
        }

        /// <summary>
        /// Project specified selection
        /// </summary>
        /// <param name="selection">enumerator specifying selceted objects</param>
        public void Project(IEnumerator<From> selection)
        {
            while (selection.MoveNext())
            {
                Map(selection.Current);
            }
        }

        /// <summary>
        /// Project specified selection
        /// </summary>
        /// <param name="selection">enumerator specifying selceted objects</param>
        public void Project(IEnumerable<From> selection)
        {
            foreach (From obj in selection)
            {
                Map(obj);
            }
        }

        /// <summary>
        /// Join this projection with another projection.
        /// Result of this join is set of objects present in both projections.
        /// </summary>
        /// <param name="prj">joined projection</param>
        public void Join<X>(Projection<X, To> prj) where X : class,IPersistent
        {
            Dictionary<To, To> join = new Dictionary<To, To>();
            foreach (To p in prj.hash.Keys)
            {
                if (hash.ContainsKey(p))
                    join[p] = p;
            }
            hash = join;
        }

        /// <summary>
        /// Get result of preceding project and join operations
        /// </summary>
        /// <returns>array of objects</returns>
        public To[] ToArray()
        {
            To[] arr = new To[hash.Count];
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
        /// Get enumerator for the result of preceding project and join operations
        /// </summary>
        /// <returns>enumerator</returns>
        public IEnumerator<To> GetEnumerator()
        {
            return hash.Keys.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
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
        public void Add(To obj)
        {
            if (obj != null)
                hash[obj] = obj;
        }

        /// <summary>
        /// Get related objects for the object obj. 
        /// It's possible to redefine this method in derived classes 
        /// to provide application specific mapping
        /// </summary>
        /// <param name="obj">object from the selection</param>
        protected void Map(From obj)
        {
            if (field == null)
            {
                Add((To)(object)obj);
                return;
            }

            object o = field.GetValue(obj);
            if (o is ILink<To>)
            {
                To[] arr = ((ILink<To>)o).ToArray();
                for (int i = 0; i < arr.Length; i++)
                {
                    Add(arr[i]);
                }
                return;
            }

            if (o is To[])
            {
                To[] arr = (To[])o;
                for (int i = 0; i < arr.Length; i++)
                {
                    Add(arr[i]);
                }
                return;
            }

            Add((To)o);
        }

        public bool IsReadOnly
        {
            get
            {
                return false;
            }
        }

        public bool Contains(To obj)
        {
            return hash.ContainsKey(obj);
        }

        public bool Remove(To obj)
        {
            return hash.Remove(obj);
        }

        public void Clear()
        {
            hash.Clear();
        }

        private Dictionary<To, To> hash = new Dictionary<To, To>();
        private FieldInfo field;
    }
}
