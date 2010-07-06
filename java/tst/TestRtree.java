import org.garret.perst.*;

class SpatialObject extends Persistent { 
    Rectangle rect;

    public String toString() { 
        return rect.toString();
    }
}

public class TestRtree extends Persistent { 
    SpatialIndex index;
    final static int nObjectsInTree = 1000;
    final static int nIterations = 100000;

    public static void main(String[] args) { 
        Storage db = StorageFactory.getInstance().createStorage();
        long start = System.currentTimeMillis();
        db.open("testrtree.dbs");
        TestRtree root = (TestRtree)db.getRoot();
        if (root == null) { 
            root = new TestRtree();
            root.index = db.createSpatialIndex();
            db.setRoot(root);
        }

        Rectangle[] rectangles = new Rectangle[nObjectsInTree];
        long key = 1999;
        for (int i = 0; i < nIterations; i++) { 
            int j = i % nObjectsInTree;
            if (i >= nObjectsInTree) { 
                Rectangle r = rectangles[j];
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
            Rectangle r = new Rectangle(top, left, bottom, right);
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
