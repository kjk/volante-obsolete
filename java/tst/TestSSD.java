// Supplier - Shipment - Detail example

import org.garret.perst.*;
import java.util.Iterator;
import java.io.*;

public class TestSSD extends Persistent {
    static class Supplier extends Persistent {
        String name;
        String location;
    }
    
    static class Detail extends Persistent {
        String id;
        float  weight;
    }
    
    static class Shipment extends Persistent { 
        Supplier supplier;
        Detail   detail;
        int      quantity;
        long     price;
    }

    FieldIndex supplierName;
    FieldIndex detailId;
    FieldIndex shipmentSupplier;
    FieldIndex shipmentDetail;
     
    static byte[] inputBuffer = new byte[256];

    static void skip(String prompt) {
        try { 
            System.out.print(prompt);
            System.in.read(inputBuffer);
        } catch (IOException x) {}
    }

    static String input(String prompt) {
        while (true) { 
            try { 
                System.out.print(prompt);
                int len = System.in.read(inputBuffer);
                String answer = new String(inputBuffer, 0, len).trim();
                if (answer.length() != 0) {
                    return answer;
                }
            } catch (IOException x) {}
        }
    }

    static long inputLong(String prompt) { 
        while (true) { 
            try { 
                return Long.parseLong(input(prompt), 10);
            } catch (NumberFormatException x) { 
                System.err.println("Invalid integer constant");
            }
        }
    }

    static double inputDouble(String prompt) { 
        while (true) { 
            try { 
                return Double.parseDouble(input(prompt));
            } catch (NumberFormatException x) { 
                System.err.println("Invalid floating point constant");
            }
        }
    }

    static public void main(String[] args) {    
        Storage db = StorageFactory.getInstance().createStorage();
        Supplier   supplier;
        Detail     detail;
        Shipment   shipment;
        Shipment[] shipments;
        Iterator   iterator;
        int        i;

        db.open("testssd.dbs");

        TestSSD root = (TestSSD)db.getRoot();
        if (root == null) { 
            root = new TestSSD();
            root.supplierName = db.createFieldIndex(Supplier.class, "name", true);
            root.detailId = db.createFieldIndex(Detail.class, "id", true);
            root.shipmentSupplier = db.createFieldIndex(Shipment.class, "supplier", false);
            root.shipmentDetail = db.createFieldIndex(Shipment.class, "detail", false);
            db.setRoot(root);
        }
        while (true) { 
            try { 
                switch ((int)inputLong("-------------------------------------\n" + 
                                       "Menu:\n" + 
                                       "1. Add supplier\n" + 
                                       "2. Add detail\n" + 
                                       "3. Add shipment\n" + 
                                       "4. List of suppliers\n" + 
                                       "5. List of details\n" + 
                                       "6. Suppliers of detail\n" + 
                                       "7. Details shipped by supplier\n" + 
                                       "8. Exit\n\n>>"))
                {
                  case 1:
                    supplier = new Supplier();
                    supplier.name = input("Supplier name: ");
                    supplier.location = input("Supplier location: ");
                    root.supplierName.put(supplier);
                    db.commit();
                    continue;
                  case 2:
                    detail = new Detail();
                    detail.id = input("Detail id: ");
                    detail.weight = (float)inputDouble("Detail weight: ");
                    root.detailId.put(detail);
                    db.commit();
                    continue;
                  case 3:
                    supplier = (Supplier)root.supplierName.get(new Key(input("Supplier name: ")));
                    if (supplier == null) { 
                        System.err.println("No such supplier!");
                        break;
                    }
                    detail = (Detail)root.detailId.get(new Key(input("Detail ID: ")));
                    if (detail == null) { 
                        System.err.println("No such detail!");
                        break;
                    }
                    shipment = new Shipment();
                    shipment.quantity = (int)inputLong("Shipment quantity: ");
                    shipment.price = inputLong("Shipment price: ");
                    shipment.detail = detail;
                    shipment.supplier = supplier;
                    root.shipmentSupplier.put(shipment);
                    root.shipmentDetail.put(shipment);
                    db.commit();
                    continue;
                  case 4:
                    iterator = root.supplierName.iterator();
                    while (iterator.hasNext()) { 
                        supplier = (Supplier)iterator.next();
                        System.out.println("Supplier name: " + supplier.name + ", supplier.location: " + supplier.location);
                    }
                    break;
                  case 5:
                    iterator = root.detailId.iterator();
                    while (iterator.hasNext()) { 
                        detail = (Detail)iterator.next();
                        System.out.println("Detail ID: " + detail.id + ", detail.weight: " + detail.weight);
                    }
                    break;
                  case 6:
                    detail = (Detail)root.detailId.get(new Key(input("Detail ID: ")));
                    if (detail == null) { 
                        System.err.println("No such detail!");
                        break;
                    }
                    shipments = (Shipment[])root.shipmentDetail.get(new Key(detail), new Key(detail));
                    for (i = 0; i < shipments.length; i++) { 
                        System.out.println("Suppplier name: " + shipments[i].supplier.name);
                    }
                    break;
                  case 7:
                    supplier = (Supplier)root.supplierName.get(new Key(input("Supplier name: ")));
                    if (supplier == null) { 
                        System.err.println("No such supplier!");
                        break;
                    }
                    shipments = (Shipment[])root.shipmentSupplier.get(new Key(supplier), new Key(supplier));
                    for (i = 0; i < shipments.length; i++) { 
                        System.out.println("Detail ID: " + shipments[i].detail.id);
                    }
                    break;
                  case 8:
                    db.close();
                    return;
                }
                skip("Press ENTER to continue...");
            } catch (StorageError x) { 
                System.out.println("Error: " + x.getMessage());
                skip("Press ENTER to continue...");
            }
        }
    }
}
