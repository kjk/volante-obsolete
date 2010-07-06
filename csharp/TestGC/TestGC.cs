using System;
using Perst;

class PObject:Persistent { 
    internal long    intKey;
    internal PObject next;
    internal String  strKey;
};

class StorageRoot:Persistent {
    internal PObject list;
    internal Index   strIndex;
    internal Index   intIndex;
}

public class TestGC { 
    const int nObjectsInTree = 10000;
    const int nIterations = 100000;

    static public void Main(String[] args) {	
        Storage db = StorageFactory.Instance.createStorage();

	    db.open("testgc.dbs");
        db.setGcThreshold(1000000);
        StorageRoot root = new StorageRoot();
        root.strIndex = db.createIndex(typeof(String), true);
        root.intIndex = db.createIndex(typeof(long), true);
        db.Root = root;
        Index intIndex = root.intIndex;
        Index strIndex = root.strIndex;
        long insKey = 1999;
        long remKey = 1999;
        int i;
        for (i = 0; i < nIterations; i++) { 
            if (i > nObjectsInTree) { 
                remKey = (3141592621L*remKey + 2718281829L) % 1000000007L;
                intIndex.remove(new Key(remKey));                
                strIndex.remove(new Key(remKey.ToString()));
            }
            PObject obj = new PObject();
            insKey = (3141592621L*insKey + 2718281829L) % 1000000007L;
            obj.intKey = insKey;
            obj.strKey = insKey.ToString();
            obj.next = new PObject();
            intIndex.put(new Key(obj.intKey), obj);                
            strIndex.put(new Key(obj.strKey), obj);
            if (i > 0) { 
                Assert.that(root.list.intKey == i-1);
            }
            root.list = new PObject();
            root.list.intKey = i;
            root.store();
            if (i % 1000 == 0) { 
                db.commit();
            }            
        }
        db.close();
    }
}
