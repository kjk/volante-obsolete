using NachoDB;
using System;
using System.Collections;

public class Supplier : Persistent 
{
    public String name;
    public String location;
#if USE_GENERICS
    public Relation<Order,Supplier> orders;
#else
    public Relation orders;
#endif
}

public class Detail : Persistent 
{
    public String id;
    public float  weight;
#if USE_GENERICS
    public Relation<Order,Detail> orders;
#else
    public Relation orders;
#endif
}

public class Order : Persistent 
{ 
#if USE_GENERICS
    public Relation<Order,Supplier> supplier;
    public Relation<Order,Detail> detail;
#else
    public Relation supplier;
    public Relation detail;
#endif
    public int      quantity;
    public long     price;

    public class QuantityComparer : IComparer 
    {
        public int Compare(object a, object b) 
        { 
            return ((Order)a).quantity - ((Order)b).quantity;
        }
    }
    public static QuantityComparer quantityComparer = new QuantityComparer();
}

public class TestSOD : Persistent 
{
#if USE_GENERICS
    public FieldIndex<string,Supplier> supplierName;
    public FieldIndex<string,Detail>   detailId;
#else
    public FieldIndex supplierName;
    public FieldIndex detailId;
#endif

    static void skip(String prompt) 
    {
        Console.Write(prompt);
        Console.ReadLine();
    }

    static String input(System.String prompt)
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

    static long inputLong(String prompt) 
    { 
        while (true) 
        { 
            try 
            { 
                return Int32.Parse(input(prompt));
            } 
            catch (FormatException) 
            { 
                Console.WriteLine("Invalid integer constant");
            }
        }
    }

    static double inputDouble(String prompt) 
    { 
        while (true) 
        { 
            try 
            { 
                return Double.Parse(input(prompt));
            } 
            catch (FormatException) 
            { 
                Console.WriteLine("Invalid floating point constant");
            }
        }
    }

    static public void Main(String[] args) 
    {	
        Storage db = StorageFactory.CreateStorage();
        Supplier   supplier;
        Detail     detail;
        Order      order;
        Order[]    orders;
#if USE_GENERICS
        Projection<Detail,Order> d2o = new Projection<Detail,Order>("orders");
        Projection<Supplier,Order> s2o = new Projection<Supplier,Order>("orders");
#else
        Projection d2o = new Projection(typeof(Detail), "orders");
        Projection s2o = new Projection(typeof(Supplier), "orders");
#endif
        int        i;

        db.Open("testsod.dbs");

        TestSOD root = (TestSOD)db.Root;
        if (root == null) 
        { 
            root = new TestSOD();
#if USE_GENERICS
            root.supplierName = db.CreateFieldIndex<string,Supplier>("name", true);
            root.detailId = db.CreateFieldIndex<string,Detail>("id", true);
#else
            root.supplierName = db.CreateFieldIndex(typeof(Supplier), "name", true);
            root.detailId = db.CreateFieldIndex(typeof(Detail), "id", true);
#endif
            db.Root = root;
        }
        while (true) 
        { 
            try 
            { 
                switch ((int)inputLong("-------------------------------------\n" + 
                    "Menu:\n" + 
                    "1. Add supplier\n" + 
                    "2. Add detail\n" + 
                    "3. Add Order\n" + 
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
#if USE_GENERICS
                        supplier.orders = db.CreateRelation<Order,Supplier>(supplier);
#else
                        supplier.orders = db.CreateRelation(supplier);
#endif
                        root.supplierName.Put(supplier);
                        db.Commit();
                        continue;
                    case 2:
                        detail = new Detail();
                        detail.id = input("Detail id: ");
                        detail.weight = (float)inputDouble("Detail weight: ");
#if USE_GENERICS
                        detail.orders = db.CreateRelation<Order,Detail>(detail);
#else
                        detail.orders = db.CreateRelation(detail);
#endif
                        root.detailId.Put(detail);
                        db.Commit();
                        continue;
                    case 3:
#if USE_GENERICS
                        supplier = root.supplierName[input("Supplier name: ")];
#else
                        supplier = (Supplier)root.supplierName[input("Supplier name: ")];
#endif
                        if (supplier == null) 
                        { 
                            Console.WriteLine("No such supplier!");
                            break;
                        }
#if USE_GENERICS
                        detail = root.detailId[input("Detail ID: ")];
#else
                        detail = (Detail)root.detailId[input("Detail ID: ")];
#endif
                        if (detail == null) 
                        { 
                            Console.WriteLine("No such detail!");
                            break;
                        }
                        order = new Order();
                        order.quantity = (int)inputLong("Order quantity: ");
                        order.price = inputLong("Order price: ");
                        order.detail = detail.orders;
                        order.supplier = supplier.orders;
                        detail.orders.Add(order);
                        supplier.orders.Add(order);
                        db.Commit();
                        continue;
                    case 4:
                        foreach (Supplier s in root.supplierName) 
                        { 
                            Console.WriteLine("Supplier name: " + s.name + ", supplier.location: " + s.location);
                        }
                        break;
                    case 5:
                        foreach (Detail d in root.detailId) 
                        {
                            Console.WriteLine("Detail ID: " + d.id + ", detail.weight: " + d.weight);
                        }
                        break;
                    case 6:
                        detail = (Detail)root.detailId[input("Detail ID: ")];
                        if (detail == null) 
                        { 
                            Console.WriteLine("No such detail!");
                            break;
                        }
                        foreach (Order o in detail.orders)
                        { 
                            supplier = (Supplier)o.supplier.Owner;
                            Console.WriteLine("Suppplier name: " + supplier.name);
                        }
                        break;
                    case 7:
#if USE_GENERICS
                        supplier = root.supplierName[input("Supplier name: ")];
#else
                        supplier = (Supplier)root.supplierName[input("Supplier name: ")];
#endif
                        if (supplier == null) 
                        { 
                            Console.WriteLine("No such supplier!");
                            break;
                        }
                        foreach (Order o in supplier.orders)
                        { 
                            detail = (Detail)o.detail.Owner;
                            Console.WriteLine("Detail ID: " + detail.id);
                        }
                        break;
                    case 8:
                        d2o.Reset();
                        d2o.Project(root.detailId.StartsWith(input("Detail ID prefix: ")));
                        s2o.Reset();
                        s2o.Project(root.supplierName.StartsWith(input("Supplier name prefix: ")));
                        s2o.Join(d2o);
#if USE_GENERICS
                        orders = s2o.ToArray();
#else
                        orders = (Order[])s2o.ToArray(typeof(Order));
#endif
                        Array.Sort(orders, 0, orders.Length, Order.quantityComparer);
                        for (i = 0; i < orders.Length; i++) 
                        { 
                            order = orders[i];
#if USE_GENERICS
                            supplier = order.supplier.Owner;
                            detail = order.detail.Owner;
#else
                            supplier = (Supplier)order.supplier.Owner;
                            detail = (Detail)order.detail.Owner;
#endif
                            Console.WriteLine("Detail ID: " + detail.id + ", supplier name: " 
                                + supplier.name + ", quantity: " + order.quantity);
                        }
                        break;           
                    case 9:
                        db.Close();
                        return;
                }
                skip("Press ENTER to continue...");
            }
            catch (StorageError x) 
            { 
                Console.WriteLine("Error: " + x.Message);
                skip("Press ENTER to continue...");
            }
        }
    }
}

