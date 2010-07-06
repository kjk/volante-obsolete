import org.garret.perst.*;

import java.util.Iterator;

public class TestR2 extends Persistent { 
    static class SpatialObject extends Persistent { 
        RectangleR2 rect;
        
        public String toString() { 
            return rect.toString();
        }
    }

    SpatialIndexR2 index;
    final static int nObjectsInTree = 1000;
    final static int nIterations = 100000;

    public static void main(String[] args) { 
        Storage db = StorageFactory.getInstance().createStorage();
        long start = System.currentTimeMillis();
        if (args.length > 0 && "noflush".equals(args[0]))
        { 
            db.setProperty("perst.file.noflush", Boolean.TRUE);
        }
        
        db.open("testr2.dbs");
        TestR2 root = (TestR2)db.getRoot();
        if (root == null) { 
            root = new TestR2();
            root.index = db.createSpatialIndexR2();
            db.setRoot(root);
        }

        RectangleR2[] rectangles = new RectangleR2[nObjectsInTree];
        long key = 1999;
        for (int i = 0; i < nIterations; i++) { 
            int j = i % nObjectsInTree;
            if (i >= nObjectsInTree) { 
                RectangleR2 r = rectangles[j];
                IPersistent[] sos = root.index.get(r);
                IPersistent po = null;
                int n = 0;
                for (int k = 0; k < sos.length; k++) { 
                    SpatialObject so = (SpatialObject)sos[k];
                    if (r.equals(so.rect)) { 
                        po = so;
                    } else { 
                        Assert.that(r.intersects(so.rect));
                    }
                }    
                Assert.that(po != null);
                for (int k = 0; k < nObjectsInTree; k++) { 
                    if (r.intersects(rectangles[k])) {
                        n += 1;
                    }
                }
                Assert.that(n == sos.length);

                Iterator iterator = root.index.iterator(r);
                for (int k = 0; iterator.hasNext(); k++) { 
                    n -= 1;
                    Assert.that(iterator.next() == sos[k]);
                }
                Assert.that(n == 0);

                root.index.remove(r, po);
                po.deallocate();
            }
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            int top = (int)(key % 1000);
            int left = (int)(key / 1000 % 1000);            
            key = (3141592621L*key + 2718281829L) % 1000000007L;
            int bottom = top + (int)(key % 100);
            int right = left + (int)(key / 100 % 100);
            SpatialObject so = new SpatialObject();
            RectangleR2 r = new RectangleR2(top, left, bottom, right);
            so.rect = r;
            rectangles[j] = r;
            root.index.put(r, so);

            if (i % 100 == 0) { 
                System.out.print("Iteration " + i + "\r");
                System.out.flush();
                db.commit();
            }
        }        
        root.index.clear();
        System.out.println("\nElapsed time " + (System.currentTimeMillis() - start));
        db.close();
    }
}
