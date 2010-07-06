import org.garret.perst.*;

import java.util.HashMap;

class L1List implements java.io.Serializable { 
    L1List next;
    Object obj;

    L1List(Object value, L1List list) { 
        obj = value;
        next = list;
    }
};

public class TestRaw extends Persistent { 
    L1List  list;
    HashMap map;
    Object  nil;

    static final int nListMembers = 100;
    static final int nHashMembers = 1000;

    public static void main(String[] args) { 
        Storage db = StorageFactory.getInstance().createStorage();
        db.setProperty("perst.serialize.transient.objects", Boolean.TRUE);
	db.open("testraw.dbs");
        TestRaw root = (TestRaw)db.getRoot();
        if (root == null) { 
            root = new TestRaw();
            L1List list = null;
            for (int i = 0; i < nListMembers; i++) { 
                list = new L1List(new Integer(i), list);
            }            
            root.list = list;
            root.map = new HashMap();
            for (int i = 0; i < nHashMembers; i++) { 
                root.map.put("key-" + i, "value-" + i);
            }
            db.setRoot(root);
            System.out.println("Initialization of database completed");
        } 
        L1List list = root.list;
        for (int i = nListMembers; --i >= 0;) { 
            Assert.that(list.obj.equals(new Integer(i)));
            list = list.next;
        }
        for (int i = nHashMembers; --i >= 0;) { 
            Assert.that(root.map.get("key-" + i).equals("value-" + i));
        }
        System.out.println("Database is OK");
        db.close();
    }
}

