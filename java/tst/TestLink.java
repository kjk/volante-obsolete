import org.garret.perst.*;
import java.io.*;

class Detail extends Persistent {
    String name;
    String color;
    double weight;
    Link   orders;
}

class Supplier extends Persistent { 
    String name;
    String address;
    Link   orders;
}

class Order extends Persistent { 
    Detail   detail;
    Supplier supplier; 
    int      quantity;
    long     price;
}

class Root extends Persistent {
    FieldIndex  details;
    FieldIndex  suppliers;
}

public class TestLink {
    static byte[] inputBuffer = new byte[256];

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

    static int inputInt(String prompt) { 
        while (true) { 
            try { 
                return Integer.parseInt(input(prompt), 10);
            } catch (NumberFormatException x) {}
        }
    }

    static double inputReal(String prompt) { 
        while (true) { 
            try { 
                return Double.parseDouble(input(prompt));
            } catch (NumberFormatException x) {}
        }
    }

    public static void main(String args[]) { 
        String name;
        Supplier supplier;
        Detail detail;
        Supplier[] suppliers;
        Detail[] details;
        Order order;
        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testlist.dbs");
        Root root = (Root)db.getRoot();
        
        if (root == null) { 
            root = new Root();
            root.details = db.createFieldIndex(Detail.class, "name", true);
            root.suppliers = db.createFieldIndex(Supplier.class, "name", true);
            db.setRoot(root);
        }
        while (true) { 
            System.out.println("------------------------------------------");
            System.out.println("1. Add supplier");
            System.out.println("2. Add detail");
            System.out.println("3. Add order");
            System.out.println("4. Search suppliers");
            System.out.println("5. Search details");
            System.out.println("6. Suppliers of detail");
            System.out.println("7. Deails shipped by supplier");
            System.out.println("8. Exit");
            String str = input("> ");
            int cmd;
            try { 
                cmd = Integer.parseInt(str, 10);
            } catch (NumberFormatException x) { 
                System.out.println("Invalid command");
                continue;
            } 
            switch (cmd) { 
              case 1:
                supplier = new Supplier();
                supplier.name = input("Supplier name: ");
                supplier.address = input("Supplier address: ");
                supplier.orders = db.createLink();
                root.suppliers.put(supplier);
                break;
              case 2:
                detail = new Detail();
                detail.name = input("Detail name: ");
                detail.weight = inputReal("Detail weight: ");
                detail.color = input("Detail color: ");
                detail.orders = db.createLink();
                root.details.put(detail);
                break;
              case 3:
                order = new Order();
                name = input("Supplier name: ");
                order.supplier = (Supplier)root.suppliers.get(new Key(name));
                if (order.supplier == null) {
                    System.out.println("No such supplier");
                    continue;
                }
                name = input("Detail name: ");
                order.detail = (Detail)root.details.get(new Key(name));
                if (order.detail == null) {
                    System.out.println("No such detail");
                    continue;
                }
                order.quantity = inputInt("Quantity: ");
                order.price = inputInt("Price: ");
                order.detail.orders.add(order);
                order.supplier.orders.add(order);
                order.detail.store();
                order.supplier.store();
                break;
              case 4:
                name = input("Supplier name prefix: ");
                suppliers = (Supplier[])root.suppliers.get(new Key(name), new Key(name + (char)255, false));
                if (suppliers.length == 0) {
                    System.out.println("No such suppliers found");
                } else {
                    for (int i = 0; i < suppliers.length; i++) { 
                        System.out.println(suppliers[i].name + '\t' + suppliers[i].address);
                    }
                }
                continue;
              case 5:
                name = input("Detail name prefix: ");
                details = (Detail[])root.details.get(new Key(name), new Key(name + (char)255, false));
                if (details.length == 0) {
                    System.out.println("No such details found");
                } else {
                    for (int i = 0; i < details.length; i++) { 
                        System.out.println(details[i].name + '\t' + details[i].weight + '\t' + details[i].color);
                    }
                }
                continue;
              case 6:
                name = input("Detail name: ");
                detail = (Detail)root.details.get(new Key(name));
                if (detail == null) { 
                    System.out.println("No such detail");
                } else {
                    for (int i = detail.orders.size(); --i >= 0;) { 
                        System.out.println(((Order)detail.orders.get(i)).supplier.name);
                    }
                }
                continue;
              case 7:
                name = input("Supplier name: ");
                supplier = (Supplier)root.suppliers.get(new Key(name));
                if (supplier == null) { 
                    System.out.println("No such supplier");
                } else {
                    for (int i = supplier.orders.size(); --i >= 0;) { 
                        System.out.println(((Order)supplier.orders.get(i)).detail.name);
                    }
                }
                continue;
              case 8:
                db.close();
                System.out.println("End of session");
                return;
              default:
                System.out.println("Invalid command");
            }
            db.commit();
        }
    }
}        



