import org.garret.perst.*;

class PObject extends Persistent { 
    long    intKey;
    PObject next;
    String  strKey;
};

class StorageRoot extends Persistent {
    PObject list;
    Index   strIndex;
    Index   intIndex;
}

public class TestGC { 
    final static int nObjectsInTree = 10000;
    final static int nIterations = 100000;

    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();

        for (int i = 0; i < args.length; i++) { 
            if ("background".equals(args[i])) { 
                db.setProperty("perst.background.gc", Boolean.TRUE);
            } else if ("altbtree".equals(args[i])) { 
                db.setProperty("perst.alternative.btree", Boolean.TRUE);
            } else { 
                System.err.println("Unrecognized option: " + args[i]);
            }
        }
        db.open("testgc.dbs");
        db.setGcThreshold(1000000);
        StorageRoot root = new StorageRoot();
        root.strIndex = db.createIndex(String.class, true);
        root.intIndex = db.createIndex(long.class, true);
        db.setRoot(root);
        Index intIndex = root.intIndex;
        Index strIndex = root.strIndex;
        long insKey = 1999;
        long remKey = 1999;
        int i;
        for (i = 0; i < nIterations; i++) { 
            if (i > nObjectsInTree) { 
                remKey = (3141592621L*remKey + 2718281829L) % 1000000007L;
                intIndex.remove(new Key(remKey));                
                strIndex.remove(new Key(Long.toString(remKey)));
            }
            PObject obj = new PObject();
            insKey = (3141592621L*insKey + 2718281829L) % 1000000007L;
            obj.intKey = insKey;
            obj.strKey = Long.toString(insKey);
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
                System.out.print("Iteration " + i + "\r");
                System.out.flush();
                db.commit();
            }            
        }
        db.close();
    }
}
