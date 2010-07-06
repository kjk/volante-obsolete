import org.garret.perst.*;

import java.util.*;

class Car extends Persistent { 
    int    hps;
    int    maxSpeed;
    int    timeTo100;
    int    options;
    String model;
    String vendor;
    String specification;

    static final int CLASS_A           = 0x00000001;
    static final int CLASS_B           = 0x00000002;
    static final int CLASS_C           = 0x00000004;
    static final int CLASS_D           = 0x00000008;

    static final int UNIVERAL          = 0x00000010;
    static final int SEDAN             = 0x00000020;
    static final int HATCHBACK         = 0x00000040;
    static final int MINIWAN           = 0x00000080;

    static final int AIR_COND          = 0x00000100;
    static final int CLIMANT_CONTROL   = 0x00000200;
    static final int SEAT_HEATING      = 0x00000400;
    static final int MIRROR_HEATING    = 0x00000800;

    static final int ABS               = 0x00001000;
    static final int ESP               = 0x00002000;
    static final int EBD               = 0x00004000;
    static final int TC                = 0x00008000;

    static final int FWD               = 0x00010000;
    static final int REAR_DRIVE        = 0x00020000;
    static final int FRONT_DRIVE       = 0x00040000;

    static final int GPS_NAVIGATION    = 0x00100000;
    static final int CD_RADIO          = 0x00200000;
    static final int CASSETTE_RADIO    = 0x00400000;
    static final int LEATHER           = 0x00800000;

    static final int XEON_LIGHTS       = 0x01000000;
    static final int LOW_PROFILE_TIRES = 0x02000000;
    static final int AUTOMATIC         = 0x04000000;

    static final int DISEL             = 0x10000000;
    static final int TURBO             = 0x20000000;
    static final int GASOLINE          = 0x40000000;
};

class Catalogue extends Persistent {
    FieldIndex modelIndex;
    BitIndex   optionIndex;
};
    

public class TestBit { 
    final static int nRecords = 1000000;
    static int pagePoolSize = 48*1024*1024;

    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testbit.dbs", pagePoolSize);

        Catalogue root = (Catalogue)db.getRoot();
        if (root == null) { 
            root = new Catalogue();
            root.optionIndex = db.createBitIndex();
            root.modelIndex = db.createFieldIndex(Car.class, "model", true);
            db.setRoot(root);
        }
        BitIndex index = root.optionIndex;
        long start = System.currentTimeMillis();
        long rnd = 1999;
        int i, n;        

        int selectedOptions = Car.TURBO|Car.DISEL|Car.FWD|Car.ABS|Car.EBD|Car.ESP|Car.AIR_COND|Car.HATCHBACK|Car.CLASS_C;
        int unselectedOptions = Car.AUTOMATIC;

        for (i = 0, n = 0; i < nRecords; i++) { 
            rnd = (3141592621L*rnd + 2718281829L) % 1000000007L;
            int options = (int)rnd;
            Car car = new Car();
            car.model = Long.toString(rnd);
            car.options = options;
            root.modelIndex.put(car);
            root.optionIndex.put(car, options);
            if ((options & selectedOptions) == selectedOptions && (options & unselectedOptions) == 0) {
                n += 1;
            }
        }
        System.out.println("Elapsed time for inserting " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");


        start = System.currentTimeMillis();
        Iterator iterator = root.optionIndex.iterator(selectedOptions, unselectedOptions);
        for (i = 0; iterator.hasNext(); i++) {
            Car car = (Car)iterator.next();
            Assert.that((car.options & selectedOptions) == selectedOptions);
            Assert.that((car.options & unselectedOptions) == 0);
        }
        System.out.println("Number of selected cars: " + i);
        Assert.that(i == n);
        System.out.println("Elapsed time for bit search through " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        iterator = root.modelIndex.iterator();        
        for (i = 0, n = 0; iterator.hasNext(); i++) { 
            Car car = (Car)iterator.next();
            root.optionIndex.remove(car);
            car.deallocate();
        }
        root.optionIndex.clear();
        System.out.println("Elapsed time for removing " + nRecords + " records: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        db.close();
    }
}
