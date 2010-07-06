using System;
using Perst;
using System.Diagnostics;

class PObject:Persistent 
{ 
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
        Storage db = StorageFactory.Instance.CreateStorage();
    
        for (int i = 0; i < args.Length; i++) 
        { 
            if ("altbtree" == args[i]) 
            { 
                db.SetProperty("perst.alternative.btree", true);
            } 
            else if ("background" == args[i]) 
            { 
                db.SetProperty("perst.background.gc", true);
            } 
            else 
            { 
                Console.WriteLine("Unrecognized option: " + args[i]);
            }
        }
        db.Open("testgc.dbs");
        db.SetGcThreshold(1000000);
        StorageRoot root = new StorageRoot();
        root.strIndex = db.CreateIndex(typeof(String), true);
        root.intIndex = db.CreateIndex(typeof(long), true);
        db.Root = root;
        Index intIndex = root.intIndex;
        Index strIndex = root.strIndex;
        long insKey = 1999;
        long remKey = 1999;
        
        for (int i = 0; i < nIterations; i++) { 
            if (i > nObjectsInTree) { 
                remKey = (3141592621L*remKey + 2718281829L) % 1000000007L;
                intIndex.Remove(new Key(remKey));                
                strIndex.Remove(new Key(remKey.ToString()));
            }
            PObject obj = new PObject();
            insKey = (3141592621L*insKey + 2718281829L) % 1000000007L;
            obj.intKey = insKey;
            obj.strKey = insKey.ToString();
            obj.next = new PObject();
            intIndex.Put(new Key(obj.intKey), obj);                
            strIndex.Put(new Key(obj.strKey), obj);
            if (i > 0) { 
                Debug.Assert(root.list.intKey == i-1);
            }
            root.list = new PObject();
            root.list.intKey = i;
            root.Store();
            if (i % 1000 == 0) { 
                db.Commit();
                Console.Write("Iteration " + i + "\r");
            }            
        }
        db.Close();
    }
}
