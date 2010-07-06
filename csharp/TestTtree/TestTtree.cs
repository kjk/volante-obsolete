using System;
using System.Collections;
using Perst;


class Name 
{ 
    public String first;
    public String last;
}


class Person :Persistent 
{ 
    public String firstName;
    public String lastName;
    public int    age;

    private Person() {}

    public Person(String firstName, String lastName, int age) 
    { 
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age; 
    }
}

class PersonList : Persistent 
{
    public SortedCollection list;
}

class NameComparator : PersistentComparator 
{ 
    public override int CompareMembers(IPersistent m1, IPersistent m2) 
    { 
        Person p1 = (Person)m1;
        Person p2 = (Person)m2;
        int diff = p1.firstName.CompareTo(p2.firstName);
        if (diff != 0) 
        { 
            return diff;
        }
        return p1.lastName.CompareTo(p2.lastName);
    }

    public override int CompareMemberWithKey(IPersistent mbr, Object key) 
    { 
        Person p = (Person)mbr;
        Name name = (Name)key;
        int diff = p.firstName.CompareTo(name.first);
        if (diff != 0) 
        { 
            return diff;
        }
        return p.lastName.CompareTo(name.last);
    }
}

public class TestTtree 
{ 
    const int nRecords = 100000;
    const int pagePoolSize = 32*1024*1024;

    static public void Main(String[] args) 
    {	
        Storage db = StorageFactory.Instance.CreateStorage();

        db.Open("testtree.dbs", pagePoolSize);
        PersonList root = (PersonList)db.Root;
        if (root == null) 
        { 
            root = new PersonList();
            root.list = db.CreateSortedCollection(new NameComparator(), true);
            db.Root = root;
        }
        SortedCollection list = root.list;
        long key = 1999;
        int i;
        DateTime start = DateTime.Now;
        for (i = 0; i < nRecords; i++) 
        { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            String str = Convert.ToString(key);
            int m = str.Length / 2;
            String firstName = str.Substring(0, m);
            String lastName = str.Substring(m);
            int age = (int)key % 100;
            Person p = new Person(firstName, lastName, age);
            list.Add(p);
        }
        db.Commit();
        Console.WriteLine("Elapsed time for inserting " + nRecords + " records: " 
            + (DateTime.Now - start) + " milliseconds");
        
        start = DateTime.Now;
        key = 1999;
        for (i = 0; i < nRecords; i++) 
        { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            String str = Convert.ToString(key);
            int m = str.Length / 2;
            Name name = new Name();
            int age = (int)key % 100;
            name.first = str.Substring(0, m);
            name.last = str.Substring(m);
            
            Person p = (Person)list[name];
            Assert.That(p != null);
            Assert.That(list.Contains(p));
            Assert.That(p.age == age);
        }
        Console.WriteLine("Elapsed time for performing " + nRecords + " index searches: " 
            + (DateTime.Now - start) + " milliseconds");
        
        start = DateTime.Now;
        Name nm = new Name();
        nm.first = nm.last = "";
        PersistentComparator comparator = list.GetComparator();
        i = 0; 
        foreach (Person p in list) 
        { 
            Assert.That(comparator.CompareMemberWithKey(p, nm) > 0);
            nm.first = p.firstName;
            nm.last = p.lastName;
            list.Remove(p);
            i += 1;
        }
        Assert.That(i == nRecords);
        Console.WriteLine("Elapsed time for removing " + nRecords + " records: " 
            + (DateTime.Now - start) + " milliseconds");
        Assert.That(list.Length == 0);
        db.Close();
    }
}
