import org.garret.perst.*;


class L2List extends PersistentResource {
    L2Elem head;
}

class L2Elem extends Persistent { 
    L2Elem next;
    L2Elem prev;
    int    count;

    public boolean recursiveLoading() { 
        return false;
    }

    void unlink() { 
        next.prev = prev;
        prev.next = next;
        next.store();
        prev.store();
    }

    void linkAfter(L2Elem elem) {         
        elem.next.prev = this;
        next = elem.next;
        elem.next = this;
        prev = elem;
        store();
        next.store();
        prev.store();
    }
}

public class TestConcur extends Thread { 
    static final int nElements = 100000;
    static final int nIterations = 100;
    static final int nThreads = 4;

    TestConcur(Storage db) { 
        this.db = db;
    }

    public void run() { 
        L2List list = (L2List)db.getRoot();
        for (int i = 0; i < nIterations; i++) { 
            long sum = 0, n = 0;
            list.sharedLock();
            L2Elem head = list.head; 
            L2Elem elem = head;
            do { 
                elem.load();
                sum += elem.count;
                n += 1;
            } while ((elem = elem.next) != head);
            Assert.that(n == nElements && sum == (long)nElements*(nElements-1)/2);
            list.unlock();
            list.exclusiveLock();
            L2Elem last = list.head.prev;
            last.unlink();
            last.linkAfter(list.head);
            list.unlock();
        }
    }

    public static void main(String[] args) throws Exception { 
        Storage db = StorageFactory.getInstance().createStorage();

        db.open("testconcur.dbs");
        L2List list = (L2List)db.getRoot();
        if (list == null) { 
            list = new L2List();
            list.head = new L2Elem();
            list.head.next = list.head.prev = list.head;
            db.setRoot(list);
            for (int i = 1; i < nElements; i++) { 
                L2Elem elem = new L2Elem();
                elem.count = i;
                elem.linkAfter(list.head); 
            }
        }
        TestConcur[] threads = new TestConcur[nThreads];
        for (int i = 0; i < nThreads; i++) { 
            threads[i] = new TestConcur(db);
            threads[i].start();
        }
        for (int i = 0; i < nThreads; i++) { 
            threads[i].join();
        }
        db.close();
    }
        
    Storage db;
}
