import org.garret.perst.*;

import java.util.Iterator;

class Name { 
    String first;
    String last;
}


class Person extends Persistent { 
    String firstName;
    String lastName;
    int    age;

    private Person() {}

    Person(String firstName, String lastName, int age) { 
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age; 
    }
}

class PersonList extends Persistent {
    SortedCollection list;
}

class NameComparator extends PersistentComparator { 
    public int compareMembers(IPersistent m1, IPersistent m2) { 
        Person p1 = (Person)m1;
        Person p2 = (Person)m2;
        int diff = p1.firstName.compareTo(p2.firstName);
        if (diff != 0) { 
            return diff;
        }
        return p1.lastName.compareTo(p2.lastName);
    }

    public int compareMemberWithKey(IPersistent mbr, Object key) { 
        Person p = (Person)mbr;
        Name name = (Name)key;
        int diff = p.firstName.compareTo(name.first);
        if (diff != 0) { 
            return diff;
        }
        return p.lastName.compareTo(name.last);
    }
}

public class TestTtree { 
    final static int nRecords = 100000;
    final static int pagePoolSize = 32*1024*1024;

    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();

        db.open("testtree.dbs", pagePoolSize);
        PersonList root = (PersonList)db.getRoot();
        if (root == null) { 
            root = new PersonList();
            root.list = db.createSortedCollection(new NameComparator(), true);
            db.setRoot(root);
        }
        SortedCollection list = root.list;
        long key = 1999;
        int i;
        long start = System.currentTimeMillis();
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            String str = Long.toString(key);
            int m = str.length() / 2;
            String firstName = str.substring(0, m);
            String lastName = str.substring(m);
            int age = (int)key % 100;
            Person p = new Person(firstName, lastName, age);
            list.add(p);
        }
        db.commit();
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        
        start = System.currentTimeMillis();
        key = 1999;
        for (i = 0; i < nRecords; i++) { 
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            String str = Long.toString(key);
            int m = str.length() / 2;
            Name name = new Name();
            int age = (int)key % 100;
            name.first = str.substring(0, m);
            name.last = str.substring(m);
            
            Person p = (Person)list.get(name);
            Assert.that(p != null);
            Assert.that(list.contains(p));
            Assert.that(p.age == age);
        }
        System.out.println("Elapsed time for performing " + nRecords + " index searches: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        
        start = System.currentTimeMillis();
        Iterator iterator = list.iterator();
        Name name = new Name();
        name.first = name.last = "";
        PersistentComparator comparator = list.getComparator();
        for (i = 0; iterator.hasNext(); i++) { 
            Person p = (Person)iterator.next();
            Assert.that(comparator.compareMemberWithKey(p, name) > 0);
            name.first = p.firstName;
            name.last = p.lastName;
            iterator.remove();
        }
        Assert.that(i == nRecords);
        System.out.println("Elapsed time for removing " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        Assert.that(!list.iterator().hasNext());
        db.close();
    }
}
