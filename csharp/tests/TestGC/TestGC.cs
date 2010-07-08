using System;
using NachoDB;
using System.Diagnostics;

class PObject:Persistent 
{ 
    internal long    intKey;
    internal PObject next;
    internal String  strKey;
};

class StorageRoot:Persistent {
    internal PObject list;
#if USE_GENERICS
    internal Index<string,PObject> strIndex;
    internal Index<long,PObject>   intIndex;
#else
    internal Index   strIndex;
    internal Index   intIndex;
#endif
}

public class TestGC { 
    const int nObjectsInTree = 10000;
    const int nIterations = 100000;

    static public void Main(String[] args) {	
        Storage db = StorageFactory.CreateStorage();
    
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
#if USE_GENERICS
        Index<string,PObject> strIndex = root.strIndex = db.CreateIndex<string,PObject>(true);
        Index<long,PObject> intIndex = root.intIndex = db.CreateIndex<long,PObject>(true);
#else
        Index strIndex = root.strIndex = db.CreateIndex(typeof(String), true);
        Index intIndex = root.intIndex = db.CreateIndex(typeof(long), true);
#endif
        db.Root = root;
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
            intIndex[obj.intKey] = obj;                
            strIndex[obj.strKey] = obj;
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
