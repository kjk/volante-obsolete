package org.garret.perst.impl;

import org.garret.perst.*;
import java.util.*;

public class RtreeR2<T extends IPersistent> extends PersistentResource implements SpatialIndexR2<T> {
    private int           height;
    private int           n;
    private RtreeR2Page   root;
    private transient int updateCounter;

    RtreeR2() {}
    RtreeR2(Storage storage) {
        super(storage);
    }

    public void put(RectangleR2 r, T obj) {
        if (root == null) { 
            root = new RtreeR2Page(getStorage(), obj, r);
            height = 1;
        } else { 
            RtreeR2Page p = root.insert(getStorage(), r, obj, height); 
            if (p != null) {
                root = new RtreeR2Page(getStorage(), root, p);
                height += 1;
            }
        }
        n += 1;
        updateCounter += 1;
        modify();
    }
    
    public int size() { 
        return n;
    }

    public void remove(RectangleR2 r, T obj) {
        if (root == null) { 
            throw new StorageError(StorageError.KEY_NOT_FOUND);
        }
        ArrayList reinsertList = new ArrayList();
        int reinsertLevel = root.remove(r, obj, height, reinsertList);
        if (reinsertLevel < 0) { 
             throw new StorageError(StorageError.KEY_NOT_FOUND);
        }        
        for (int i = reinsertList.size(); --i >= 0;) {
            RtreeR2Page p = (RtreeR2Page)reinsertList.get(i);
            for (int j = 0, n = p.n; j < n; j++) { 
                RtreeR2Page q = root.insert(getStorage(), p.b[j], p.branch.get(j), height - reinsertLevel); 
                if (q != null) { 
                    // root splitted
                    root = new RtreeR2Page(getStorage(), root, q);
                    height += 1;
                }
            }
            reinsertLevel -= 1;
            p.deallocate();
        }
        if (root.n == 1 && height > 1) { 
            RtreeR2Page newRoot = (RtreeR2Page)root.branch.get(0);
            root.deallocate();
            root = newRoot;
            height -= 1;
        }
        n -= 1;
        updateCounter += 1;
        modify();
    }
    
    public IPersistent[] get(RectangleR2 r) {
        ArrayList result = new ArrayList();
        if (root != null) { 
            root.find(r, result, height);
        }
        return (IPersistent[])result.toArray(new IPersistent[result.size()]);
    }

    public ArrayList<T> getList(RectangleR2 r) { 
        ArrayList<T> result = new ArrayList<T>();
        if (root != null) { 
            root.find(r, result, height);
        }
        return result;
    }

    public RectangleR2 getWrappingRectangle() {
        if (root != null) { 
            return root.cover();
        }
        return null;
    }

    public void clear() {
        if (root != null) { 
            root.purge(height);
            root = null;
        }
        height = 0;
        n = 0;
        updateCounter += 1;
        modify();
    }

    public void deallocate() {
        clear();
        super.deallocate();
    }


    public Iterator<T> iterator(RectangleR2 r) { 
        return new RtreeIterator(r);
    }

    class RtreeIterator<T extends IPersistent> implements Iterator<T> { 
        private RtreeR2Page[] pageStack;
        private int[]         posStack;
        private int           sp;
        private RectangleR2   r;
        private int           counter;

        RtreeIterator(RectangleR2 rect) {             
            pageStack = new RtreeR2Page[height];
            posStack = new int[height];
            r = rect;
            sp = 0;
            counter = updateCounter;
            RtreeR2Page pg = root;
            if (pg != null) { 
              push:
                while (true) { 
                    for (int i = 0; i < pg.n; i++) { 
                        if (rect.intersects(pg.b[i])) { 
                            posStack[sp] = i;
                            pageStack[sp] = pg;
                            if (++sp == pageStack.length) { 
                                return;
                            }
                            pg = (RtreeR2Page)pg.branch.get(i);
                            continue push;
                        }
                    }
                    popNext();
                    return;
                }
            }
        }

        public void remove() { 
            throw new UnsupportedOperationException();
        }

        public boolean hasNext() {
            return sp > 0 && posStack[sp-1] < pageStack[sp-1].n;
        }
        
        public T next() {
            if (!hasNext()) { 
                throw new NoSuchElementException();
            }
            if (counter != updateCounter) { 
                throw new ConcurrentModificationException();
            }
            int i = posStack[sp-1];   
            RtreeR2Page pg = pageStack[sp-1];
            T curr = (T)pg.branch.get(i);
            while (++i < pg.n) { 
                if (r.intersects(pg.b[i])) { 
                    posStack[sp-1] = i;
                    return curr;
                }
            }
            sp -= 1;
            popNext();
            return curr;
        }

        void popNext() { 
          pop:
            while (sp != 0) { 
                sp -= 1;
                int i = posStack[sp];
                RtreeR2Page pg = pageStack[sp];
                while (++i < pg.n) { 
                    if (r.intersects(pg.b[i])) {
                        posStack[sp] = i; 
                        sp += 1;
                      push:
                        while (true) { 
                            pg = (RtreeR2Page)pg.branch.get(i);
                            for (i = 0; i < pg.n; i++) { 
                                if (r.intersects(pg.b[i])) { 
                                    posStack[sp] = i;
                                    pageStack[sp] = pg;
                                    if (++sp == pageStack.length) { 
                                        return;
                                    }
                                    continue push;
                                }
                            }
                            continue pop;
                        }
                    }
                }
            }
        }
    }
}
    

