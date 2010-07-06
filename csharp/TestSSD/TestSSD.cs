using Perst;
using System;

class Supplier : Persistent {
    public String name;
    public String location;
}

class Detail : Persistent {
    public String id;
    public float  weight;
}

class Shipment : Persistent { 
    public Supplier supplier;
    public Detail   detail;
    public int      quantity;
    public long     price;
}

public class TestSSD : Persistent {
    public FieldIndex supplierName;
    public FieldIndex detailId;
    public FieldIndex shipmentSupplier;
    public FieldIndex shipmentDetail;
     

    static void skip(String prompt) {
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

    static long inputLong(String prompt) { 
        while (true) { 
            try { 
                return Int32.Parse(input(prompt));
            } catch (FormatException) { 
                Console.WriteLine("Invalid integer constant");
            }
        }
    }

    static double inputDouble(String prompt) { 
        while (true) { 
            try { 
                return Double.Parse(input(prompt));
            } catch (FormatException) { 
                Console.WriteLine("Invalid floating point constant");
            }
        }
    }

    static public void Main(String[] args) {	
        Storage db = StorageFactory.Instance.CreateStorage();
        Supplier   supplier;
        Detail     detail;
        Shipment   shipment;
        Shipment[] shipments;
        int        i;

	db.Open("testssd.dbs");

        TestSSD root = (TestSSD)db.Root;
        if (root == null) { 
            root = new TestSSD();
            root.supplierName = db.CreateFieldIndex(typeof(Supplier), "name", true);
            root.detailId = db.CreateFieldIndex(typeof(Detail), "id", true);
            root.shipmentSupplier = db.CreateFieldIndex(typeof(Shipment), "supplier", false);
            root.shipmentDetail = db.CreateFieldIndex(typeof(Shipment), "detail", false);
            db.Root = root;
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
                    root.supplierName.Put(supplier);
                    db.Commit();
                    continue;
                  case 2:
                    detail = new Detail();
                    detail.id = input("Detail id: ");
                    detail.weight = (float)inputDouble("Detail weight: ");
                    root.detailId.Put(detail);
                    db.Commit();
                    continue;
                  case 3:
                    supplier = (Supplier)root.supplierName.Get(new Key(input("Supplier name: ")));
                    if (supplier == null) { 
                        Console.WriteLine("No such supplier!");
                        break;
                    }
                    detail = (Detail)root.detailId.Get(new Key(input("Detail ID: ")));
                    if (detail == null) { 
                        Console.WriteLine("No such detail!");
                        break;
                    }
                    shipment = new Shipment();
                    shipment.quantity = (int)inputLong("Shipment quantity: ");
                    shipment.price = inputLong("Shipment price: ");
                    shipment.detail = detail;
                    shipment.supplier = supplier;
                    root.shipmentSupplier.Put(shipment);
                    root.shipmentDetail.Put(shipment);
                    db.Commit();
                    continue;
                  case 4:
                    foreach (Supplier s in root.supplierName) { 
                        Console.WriteLine("Supplier name: " + s.name + ", supplier.location: " + s.location);
                    }
                    break;
                  case 5:
                    foreach (Detail d in root.detailId) {
                        Console.WriteLine("Detail ID: " + d.id + ", detail.weight: " + d.weight);
                    }
                    break;
                  case 6:
                    detail = (Detail)root.detailId.Get(new Key(input("Detail ID: ")));
                    if (detail == null) { 
                        Console.WriteLine("No such detail!");
                        break;
                    }
                    shipments = (Shipment[])root.shipmentDetail.Get(new Key(detail), new Key(detail));
                    for (i = 0; i < shipments.Length; i++) { 
                        Console.WriteLine("Suppplier name: " + shipments[i].supplier.name);
                    }
                    break;
                  case 7:
                    supplier = (Supplier)root.supplierName.Get(new Key(input("Supplier name: ")));
                    if (supplier == null) { 
                        Console.WriteLine("No such supplier!");
                        break;
                    }
                    shipments = (Shipment[])root.shipmentSupplier.Get(new Key(supplier), new Key(supplier));
                    for (i = 0; i < shipments.Length; i++) { 
                        Console.WriteLine("Detail ID: " + shipments[i].detail.id);
                    }
                    break;
                  case 8:
                    db.Close();
                    return;
                }
                skip("Press ENTER to continue...");
            } catch (StorageError x) { 
                Console.WriteLine("Error: " + x.Message);
                skip("Press ENTER to continue...");
            }
        }
    }
}

