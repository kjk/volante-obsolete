// Supplier - Order - Detail example
// This example illustrates alternative apporach for implementing many-to-many relations
// based on using Projection class. See aslo TestSSD example.

import org.garret.perst.*;
import java.util.*;
import java.io.*;

public class TestSOD extends Persistent {
    static class Supplier extends Persistent {
        String   name;
        String   location;
        Relation orders;
    }
    
    static class Detail extends Persistent {
        String   id;
        float    weight;
        Relation orders;
    }
    
    static class Order extends Persistent { 
        Relation supplier;
        Relation detail;
        int      quantity;
        long     price;
    }

    FieldIndex supplierName;
    FieldIndex detailId;
     
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
        Order      order;
        Order[]    orders;
        Iterator   iterator;
        Projection d2o = new Projection(Detail.class, "orders");
        Projection s2o = new Projection(Supplier.class, "orders");
        int        i;

        db.open("testsod.dbs");

        TestSOD root = (TestSOD)db.getRoot();
        if (root == null) { 
            root = new TestSOD();
            root.supplierName = db.createFieldIndex(Supplier.class, "name", true);
            root.detailId = db.createFieldIndex(Detail.class, "id", true);
            db.setRoot(root);
        }
        while (true) { 
            try { 
                switch ((int)inputLong("-------------------------------------\n" + 
                                       "Menu:\n" + 
                                       "1. Add supplier\n" + 
                                       "2. Add detail\n" + 
                                       "3. Add order\n" + 
                                       "4. List of suppliers\n" + 
                                       "5. List of details\n" + 
                                       "6. Suppliers of detail\n" + 
                                       "7. Details shipped by supplier\n" + 
                                       "8. Orders for detail of supplier\n" + 
                                       "9. Exit\n\n>>"))
                {
                  case 1:
                    supplier = new Supplier();
                    supplier.name = input("Supplier name: ");
                    supplier.location = input("Supplier location: ");
                    supplier.orders = db.createRelation(supplier);
                    root.supplierName.put(supplier);
                    db.commit();
                    continue;
                  case 2:
                    detail = new Detail();
                    detail.id = input("Detail id: ");
                    detail.weight = (float)inputDouble("Detail weight: ");
                    detail.orders = db.createRelation(detail);
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
                    order = new Order();
                    order.quantity = (int)inputLong("Order quantity: ");
                    order.price = inputLong("Order price: ");
                    order.detail = detail.orders;
                    order.supplier = supplier.orders;
                    detail.orders.add(order);
                    supplier.orders.add(order);
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
                    iterator = detail.orders.iterator();
                    while (iterator.hasNext()) { 
                        order = (Order)iterator.next();
                        supplier = (Supplier)order.supplier.getOwner();
                        System.out.println("Suppplier name: " + supplier.name);
                    }
                    break;
                  case 7:
                    supplier = (Supplier)root.supplierName.get(new Key(input("Supplier name: ")));
                    if (supplier == null) { 
                        System.err.println("No such supplier!");
                        break;
                    }
                    iterator = supplier.orders.iterator();
                    while (iterator.hasNext()) { 
                        order = (Order)iterator.next();
                        detail = (Detail)order.detail.getOwner();
                        System.out.println("Detail ID: " + detail.id);
                    }
                    break;
                  case 8:
                    d2o.reset();
                    d2o.project(root.detailId.getPrefix(input("Detail ID prefix: ")));
                    s2o.reset();
                    s2o.project(root.supplierName.getPrefix(input("Supplier name prefix: ")));
                    s2o.join(d2o);
                    orders = (Order[])s2o.toArray(new Order[s2o.size()]);
                    Arrays.sort(orders, new Comparator() { 
                        public int compare(Object o1, Object o2) {
                            return ((Order)o1).quantity - ((Order)o2).quantity;
                        }
                    });
                    for (i = 0; i < orders.length; i++) { 
                        order = orders[i];
                        supplier = (Supplier)order.supplier.getOwner();
                        detail = (Detail)order.detail.getOwner();
                        System.out.println("Detail ID: " + detail.id + ", supplier name: " 
                                           + supplier.name + ", quantity: " + order.quantity);
                    }
                    break;           
                  case 9:
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
