package org.garret.perst.impl;

import org.garret.perst.*;
import java.util.ArrayList;

public class Rtree<T extends IPersistent> extends PersistentResource implements SpatialIndex<T> {
    private int       height;
    private int       n;
    private RtreePage root;

    Rtree() {}

    public void put(Rectangle r, T obj) {
        if (root == null) { 
            root = new RtreePage(obj, r);
            height = 1;
        } else { 
            RtreePage p = root.insert(r, obj, height); 
            if (p != null) {
                root = new RtreePage(root, p);
                height += 1;
            }
        }
        n += 1;
        modify();
    }
    
    public int size() { 
        return n;
    }

    public void remove(Rectangle r, T obj) {
        if (root == null) { 
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
        ArrayList reinsertList = new ArrayList();
        int reinsertLevel = root.remove(r, obj, height, reinsertList);
        if (reinsertLevel < 0) { 
             throw new StorageError(StorageError.KEY_NOT_FOUND);
        }        
        for (int i = reinsertList.size(); --i >= 0;) {
            RtreePage p = (RtreePage)reinsertList.get(i);
            for (int j = 0, n = p.n; j < n; j++) { 
                RtreePage q = root.insert(p.b[j], p.b[j].p, height - reinsertLevel); 
                if (q != null) { 
                    // root splitted
                    root = new RtreePage(root, q);
                    height += 1;
                }
            }
            reinsertLevel -= 1;
            p.deallocate();
        }
        if (root.n == 1 && height > 1) { 
            RtreePage newRoot = (RtreePage)root.b[0].p;
            root.deallocate();
            root = newRoot;
            height -= 1;
        }
        n -= 1;
        modify();
    }
    
    public IPersistent[] get(Rectangle r) {
        ArrayList<T> result = find(r);
        return (IPersistent[])result.toArray(new IPersistent[result.size()]);
    }

    public ArrayList<T> find(Rectangle r) { 
        ArrayList<T> result = new ArrayList<T>();
        if (root != null) { 
            root.find(r, result, height);
        }
        return result;
    }
    

    public void clear() {
        if (root != null) { 
            root.purge(height);
            root = null;
        }
        height = 0;
        n = 0;
        modify();
    }

    public void deallocate() {
        clear();
        super.deallocate();
    }
}
    

