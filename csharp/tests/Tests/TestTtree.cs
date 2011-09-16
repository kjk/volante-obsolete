namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public class TestTtreeResult : TestResult
    {
        public TimeSpan InsertTime;
        public TimeSpan IndexSearchTime;
        public TimeSpan RemoveTime;
    }

    public class TestTtree : ITest
    {
        class Name
        {
            public String first;
            public String last;

            public Name()
            {
            }

            public Name(Person p)
            {
                first = p.first;
                last = p.last;
            }

            public Name(long key)
            {
                String str = Convert.ToString(key);
                int m = str.Length / 2;
                this.first = str.Substring(0, m);
                this.last = str.Substring(m);
            }
        }

        class Person : Persistent
        {
            public String first;
            public String last;
            public int age;

            private Person() { }
            public Person(long key)
            {
                String str = Convert.ToString(key);
                int m = str.Length / 2;
                this.first = str.Substring(0, m);
                this.last = str.Substring(m);
                this.age = (int)key % 100;
            }

            public Person(String firstName, String lastName, int age)
            {
                this.first = firstName;
                this.last = lastName;
                this.age = age;
            }
        }

        class PersonList : Persistent
        {
            public ISortedCollection<Name, Person> list;
        }

        class NameComparator : PersistentComparator<Name, Person>
        {
            public override int CompareMembers(Person p1, Person p2)
            {
                int diff = p1.first.CompareTo(p2.first);
                if (diff != 0)
                    return diff;
                return p1.last.CompareTo(p2.last);
            }

            public override int CompareMemberWithKey(Person p, Name name)
            {
                int diff = p.first.CompareTo(name.first);
                if (diff != 0)
                    return diff;
                return p.last.CompareTo(name.last);
            }
        }

        void PopulateIndex(ISortedCollection<Name, Person> list, int count)
        {
            Person firstPerson = null;
            foreach (var key in Tests.KeySeq(count))
            {
                Person p = new Person(key);
                if (null == firstPerson)
                    firstPerson = p;
                list.Add(p);
            }
            list.Add(firstPerson);
        }

        public void Run(TestConfig config)
        {
            int i;
            int count = config.Count;
            var res = new TestTtreeResult();
            DateTime start = DateTime.Now;
            IDatabase db = config.GetDatabase();
            PersonList root = (PersonList)db.Root;
            Tests.Assert(root == null);
            root = new PersonList();
            root.list = db.CreateSortedCollection<Name, Person>(new NameComparator(), IndexType.Unique);
            db.Root = root;
            ISortedCollection<Name, Person> list = root.list;
            Tests.Assert(!list.IsReadOnly);
            Tests.Assert(!list.RecursiveLoading());
            PopulateIndex(list, count);
            db.Commit();
            res.InsertTime = DateTime.Now - start;

            start = DateTime.Now;
            foreach(var key in Tests.KeySeq(count))
            {
                Name name = new Name(key);
                int age = (int)key % 100;

                Person p = list[name];
                Tests.Assert(p != null);
                Tests.Assert(list.Contains(p));
                Tests.Assert(p.age == age);
            }
            res.IndexSearchTime = DateTime.Now - start;

            start = DateTime.Now;
            Name nm = new Name();
            nm.first = nm.last = "";
            PersistentComparator<Name, Person> comparator = list.GetComparator();
            i = 0;
            foreach (Person p in list)
            {
                Tests.Assert(comparator.CompareMemberWithKey(p, nm) > 0);
                nm.first = p.first;
                nm.last = p.last;
                Tests.Assert(list.Remove(p));
                i += 1;
            }
            Tests.Assert(i == count);
            res.RemoveTime = DateTime.Now - start;
            Tests.Assert(list.Count == 0);
            PopulateIndex(list, count);

            Person[] els = list.ToArray();
            Tests.Assert(els.Length == list.Count);
            Name firstKey = new Name(els[0]);
            Name lastKey = new Name(els[els.Length - 1]);
            Name midKey = new Name(els[els.Length / 2]);
            Person[] els2 = list[firstKey, lastKey];
            Tests.Assert(els.Length == els2.Length);
            var e = list.Range(firstKey, midKey).GetEnumerator();
            TestEnumerator(e);
            e = list.GetEnumerator(midKey, lastKey);
            TestEnumerator(e);

            foreach (var key in Tests.KeySeq(count))
            {
                var p = RemoveEl(els, key);
                Tests.Assert(list.Contains(p));
                Tests.Assert(list.Remove(p));
                Tests.Assert(!list.Contains(p));
            }
            Tests.Assert(null == list.Get(firstKey));
            Tests.Assert(null == list.Get(new Name(-123345)));
            db.Commit();
            PopulateIndex(list, 20);
            Tests.Assert(20 == list.Count);
            Tests.Assert(null == list.Get(new Name(-123456)));
            var arr = list.ToArray();
            Tests.Assert(20 == arr.Length);
            Person pTmp = arr[0];
            list.Clear();
            Tests.Assert(!list.Remove(pTmp));
            list.Deallocate();
            db.Commit();
            db.Close();
        }

        void TestEnumerator(IEnumerator<Person> e)
        {
            while (e.MoveNext())
            {
                Tests.Assert(null != e.Current);
            }
            Tests.Assert(!e.MoveNext());
            Tests.AssertException<InvalidOperationException>(
                () => { var el = e.Current; });
            e.Reset();
            Tests.Assert(e.MoveNext());
        }

        Person RemoveEl(Person[] els, long key)
        {
            int pos = (int)(key % els.Length);
            while (els[pos] == null)
            {
                pos = (pos + 1) % els.Length;
            }
            Person ret = els[pos];
            els[pos] = null;
            return ret;
        }
    }
}
