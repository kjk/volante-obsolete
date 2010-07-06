using System;
using Perst;

class Detail : Persistent 
{
    internal String name;
    internal String color;
    internal double weight;
    internal Link   orders;
}

class Supplier : Persistent 
{ 
    internal String name;
    internal String address;
    internal Link   orders;
}

class Order : Persistent 
{ 
    internal Detail   detail;
    internal Supplier supplier; 
    internal int      quantity;
    internal long     price;
}

class Root : Persistent 
{
    internal FieldIndex  details;
    internal FieldIndex  suppliers;
}

public class TestLink
{
    internal static System.String input(System.String prompt)
    {
        while (true)
        {
            Console.Write(prompt);
            String line = Console.ReadLine().Trim();
            if (line.Length != 0) 
            { 
                return line;
            }
        }
    }

    static int inputInt(String prompt) { 
        while (true) { 
            try { 
                return Int32.Parse(input(prompt));
            } catch (Exception) {}
        }
    }

    static double inputReal(String prompt) { 
        while (true) { 
            try { 
                return Double.Parse(input(prompt));
            } catch (Exception) {}
        }
    }

    public static void Main(String[] args) { 
        String name;
        Supplier supplier;
        Detail detail;
        Supplier[] suppliers;
        Detail[] details;
        Order order;
        Storage db = StorageFactory.Instance.createStorage();
        db.open("testlist.dbs");
        Root root = (Root)db.Root;
        
        if (root == null) { 
            root = new Root();
            root.details = db.createFieldIndex(typeof(Detail), "name", true);
            root.suppliers = db.createFieldIndex(typeof(Supplier), "name", true);
            db.Root = root;
        }
        while (true) { 
            Console.WriteLine("------------------------------------------");
            Console.WriteLine("1. Add supplier");
            Console.WriteLine("2. Add detail");
            Console.WriteLine("3. Add order");
            Console.WriteLine("4. Search suppliers");
            Console.WriteLine("5. Search details");
            Console.WriteLine("6. Suppliers of detail");
            Console.WriteLine("7. Deails shipped by supplier");
            Console.WriteLine("8. Exit");
            String str = input("> ");
            int cmd;
            try { 
                cmd = Int32.Parse(str);
            } catch (Exception x) { 
                Console.WriteLine("Invalid command");
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
                    Console.WriteLine("No such supplier");
                    continue;
                }
                name = input("Detail name: ");
                order.detail = (Detail)root.details.get(new Key(name));
                if (order.detail == null) {
                    Console.WriteLine("No such detail");
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
                if (suppliers.Length == 0) {
                    Console.WriteLine("No such suppliers found");
                } else {
                    for (int i = 0; i < suppliers.Length; i++) { 
                        Console.WriteLine(suppliers[i].name + '\t' + suppliers[i].address);
                    }
                }
                continue;
              case 5:
                name = input("Detail name prefix: ");
                details = (Detail[])root.details.get(new Key(name), new Key(name + (char)255, false));
                if (details.Length == 0) {
                    Console.WriteLine("No such details found");
                } else {
                    for (int i = 0; i < details.Length; i++) { 
                        Console.WriteLine(details[i].name + '\t' + details[i].weight + '\t' + details[i].color);
                    }
                }
                continue;
              case 6:
                name = input("Detail name: ");
                detail = (Detail)root.details.get(new Key(name));
                if (detail == null) { 
                    Console.WriteLine("No such detail");
                } else {
                    for (int i = detail.orders.size(); --i >= 0;) { 
                        Console.WriteLine(((Order)detail.orders.get(i)).supplier.name);
                    }
                }
                continue;
              case 7:
                name = input("Supplier name: ");
                supplier = (Supplier)root.suppliers.get(new Key(name));
                if (supplier == null) { 
                    Console.WriteLine("No such supplier");
                } else {
                    for (int i = supplier.orders.size(); --i >= 0;) { 
                        Console.WriteLine(((Order)supplier.orders.get(i)).detail.name);
                    }
                }
                continue;
              case 8:
                db.close();
                Console.WriteLine("End of session");
                return;
              default:
                Console.WriteLine("Invalid command");
                continue;
            }
            db.commit();
        }
    }
}
