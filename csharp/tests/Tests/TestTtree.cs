namespace Volante
{
    using System;

    public class TestTtreeResult : TestResult
    {
        public TimeSpan InsertTime;
        public TimeSpan IndexSearchTime;
        public TimeSpan RemoveTime;
    }

    public class TestTtree
    {
        class Name
        {
            public String first;
            public String last;
        }

        class Person : Persistent
        {
            public String firstName;
            public String lastName;
            public int age;

            private Person() { }

            public Person(String firstName, String lastName, int age)
            {
                this.firstName = firstName;
                this.lastName = lastName;
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
                int diff = p1.firstName.CompareTo(p2.firstName);
                if (diff != 0)
                {
                    return diff;
                }
                return p1.lastName.CompareTo(p2.lastName);
            }

            public override int CompareMemberWithKey(Person p, Name name)
            {
                int diff = p.firstName.CompareTo(name.first);
                if (diff != 0)
                {
                    return diff;
                }
                return p.lastName.CompareTo(name.last);
            }
        }

        const int pagePoolSize = 32 * 1024 * 1024;

        static public TestTtreeResult Run(int count)
        {
            int i;
            var res = new TestTtreeResult()
            {
                Count = count,
                TestName = "TestTtree"
            };

            string dbName = "testtree.dbs";
            Tests.SafeDeleteFile(dbName);

            DateTime tStart = DateTime.Now;
            DateTime start = DateTime.Now;
            IStorage db = StorageFactory.CreateStorage();
            db.Open(dbName, pagePoolSize);
            PersonList root = (PersonList)db.Root;
            Tests.Assert(root == null);
            root = new PersonList();
            root.list = db.CreateSortedCollection<Name, Person>(new NameComparator(), true);
            db.Root = root;
            ISortedCollection<Name, Person> list = root.list;
            long key = 1999;
            for (i = 0; i < count; i++)
            {
                String str = Convert.ToString(key);
                int m = str.Length / 2;
                String firstName = str.Substring(0, m);
                String lastName = str.Substring(m);
                int age = (int)key % 100;
                Person p = new Person(firstName, lastName, age);
                list.Add(p);
                key = (3141592621L * key + 2718281829L) % 1000000007L;
            }
            db.Commit();
            res.InsertTime = DateTime.Now - start;

            start = DateTime.Now;
            key = 1999;
            for (i = 0; i < count; i++)
            {
                String str = Convert.ToString(key);
                int m = str.Length / 2;
                Name name = new Name();
                int age = (int)key % 100;
                name.first = str.Substring(0, m);
                name.last = str.Substring(m);

                Person p = list[name];
                Tests.Assert(p != null);
                Tests.Assert(list.Contains(p));
                Tests.Assert(p.age == age);
                key = (3141592621L * key + 2718281829L) % 1000000007L;
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
                nm.first = p.firstName;
                nm.last = p.lastName;
                list.Remove(p);
                i += 1;
            }
            Tests.Assert(i == count);
            res.RemoveTime = DateTime.Now - start;
            Tests.Assert(list.Count == 0);
            db.Close();
            res.ExecutionTime = DateTime.Now - tStart;
            res.Ok = Tests.FinalizeTest();
            return res;
        }
    }
}
