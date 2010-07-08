using System;
using NachoDB;

class Detail : Persistent 
{
    internal String name;
    internal String color;
    internal double weight;
#if USE_GENERICS
    internal Link<Order> orders;
#else
    internal Link   orders;
#endif
}

class Supplier : Persistent 
{ 
    internal String name;
    internal String address;
#if USE_GENERICS
    internal Link<Order> orders;
#else
    internal Link   orders;
#endif
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
#if USE_GENERICS
    internal FieldIndex<string,Detail> details;
    internal FieldIndex<string,Supplier> suppliers;
#else
    internal FieldIndex  details;
    internal FieldIndex  suppliers;
#endif
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
        Storage db = StorageFactory.CreateStorage();
        db.Open("testlist.dbs");
        Root root = (Root)db.Root;
        
        if (root == null) { 
            root = new Root();
#if USE_GENERICS
            root.details = db.CreateFieldIndex<string,Detail>("name", true);
            root.suppliers = db.CreateFieldIndex<string,Supplier>("name", true);
#else
            root.details = db.CreateFieldIndex(typeof(Detail), "name", true);
            root.suppliers = db.CreateFieldIndex(typeof(Supplier), "name", true);
#endif
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
            } catch (Exception) { 
                Console.WriteLine("Invalid command");
                continue;
            } 
            switch (cmd) { 
              case 1:
                supplier = new Supplier();
                supplier.name = input("Supplier name: ");
                supplier.address = input("Supplier address: ");
#if USE_GENERICS
                supplier.orders = db.CreateLink<Order>();
#else
                supplier.orders = db.CreateLink();
#endif
                root.suppliers.Put(supplier);
                break;
              case 2:
                detail = new Detail();
                detail.name = input("Detail name: ");
                detail.weight = inputReal("Detail weight: ");
                detail.color = input("Detail color: ");
#if USE_GENERICS
                detail.orders = db.CreateLink<Order>();
#else
                detail.orders = db.CreateLink();
#endif
                root.details.Put(detail);
                break;
              case 3:
                order = new Order();
                name = input("Supplier name: ");
#if USE_GENERICS
                order.supplier = root.suppliers[name];
#else
                order.supplier = (Supplier)root.suppliers[name];
#endif
                if (order.supplier == null) {
                    Console.WriteLine("No such supplier");
                    continue;
                }
                name = input("Detail name: ");
#if USE_GENERICS
                order.detail = root.details[name];
#else
                order.detail = (Detail)root.details[name];
#endif
                if (order.detail == null) {
                    Console.WriteLine("No such detail");
                    continue;
                }
                order.quantity = inputInt("Quantity: ");
                order.price = inputInt("Price: ");
                order.detail.orders.Add(order);
                order.supplier.orders.Add(order);
                order.detail.Store();
                order.supplier.Store();
                break;
              case 4:
                name = input("Supplier name prefix: ");
#if USE_GENERICS
                suppliers = root.suppliers.Get(new Key(name), new Key(name + (char)255, false));
#else
                suppliers = (Supplier[])root.suppliers.Get(new Key(name), new Key(name + (char)255, false));
#endif
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
#if USE_GENERICS
                details = root.details.Get(new Key(name), new Key(name + (char)255, false));
#else
                details = (Detail[])root.details.Get(new Key(name), new Key(name + (char)255, false));
#endif
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
#if USE_GENERICS
                detail = (Detail)root.details[name];
#else
                detail = (Detail)root.details[name];
#endif
                if (detail == null) { 
                    Console.WriteLine("No such detail");
                } else {
                    for (int i = detail.orders.Length; --i >= 0;) { 
                        Console.WriteLine(((Order)detail.orders[i]).supplier.name);
                    }
                }
                continue;
              case 7:
                name = input("Supplier name: ");
#if USE_GENERICS
                supplier = (Supplier)root.suppliers[name];
#else
                supplier = (Supplier)root.suppliers[name];
#endif
                if (supplier == null) { 
                    Console.WriteLine("No such supplier");
                } else {
                    for (int i = supplier.orders.Length; --i >= 0;) { 
                        Console.WriteLine(((Order)supplier.orders[i]).detail.name);
                    }
                }
                continue;
              case 8:
                db.Close();
                Console.WriteLine("End of session");
                return;
              default:
                Console.WriteLine("Invalid command");
                continue;
            }
            db.Commit();
        }
    }
}
