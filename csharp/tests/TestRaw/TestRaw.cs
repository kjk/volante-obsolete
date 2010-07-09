using System;
using NachoDB;
using System.Collections;
using System.Diagnostics;

[Serializable()]      
class L1List 
{ 
    internal L1List next;
    internal Object obj;

    internal L1List(Object val, L1List list) 
    { 
        obj = val;
        next = list;
    }
}

public class TestRaw : Persistent 
{ 
    L1List    list;
    Hashtable map;
    Object    nil;

    const int nListMembers = 100;
    const int nHashMembers = 1000;

    public static void Main(String[] args) 
    { 
        Storage db = StorageFactory.CreateStorage();
        db.SerializeTransientObjects = true;
        db.Open("testraw.dbs");
        TestRaw root = (TestRaw)db.Root;
        if (root == null) 
        { 
            root = new TestRaw();
            L1List list = null;
            for (int i = 0; i < nListMembers; i++) 
            { 
                list = new L1List(i, list);
            }            
            root.list = list;
            root.map = new Hashtable();
            for (int i = 0; i < nHashMembers; i++) 
            { 
                root.map["key-" + i] = "value-" + i;
            }
            db.Root = root;
            Console.WriteLine("Initialization of database completed");
        } 
        L1List elem = root.list;
        for (int i = nListMembers; --i >= 0;) 
        { 
            Debug.Assert(elem.obj.Equals(i));
            elem = elem.next;
        }
        for (int i = nHashMembers; --i >= 0;) 
        { 
            Debug.Assert(root.map["key-" + i].Equals("value-" + i));
        }
        Console.WriteLine("Database is OK");
        db.Close();
    }
}

