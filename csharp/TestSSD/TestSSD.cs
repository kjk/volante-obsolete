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
                Console.Error.WriteLine("Invalid integer constant");
            }
        }
    }

    static double inputDouble(String prompt) { 
        while (true) { 
            try { 
                return Double.Parse(input(prompt));
            } catch (FormatException) { 
                Console.Error.WriteLine("Invalid floating point constant");
            }
        }
    }

    static public void Main(String[] args) {	
        Storage db = StorageFactory.Instance.createStorage();
        Supplier   supplier;
        Detail     detail;
        Shipment   shipment;
        Shipment[] shipments;
        int        i;

	db.open("testssd.dbs");

        TestSSD root = (TestSSD)db.Root;
        if (root == null) { 
            root = new TestSSD();
            root.supplierName = db.createFieldIndex(typeof(Supplier), "name", true);
            root.detailId = db.createFieldIndex(typeof(Detail), "id", true);
            root.shipmentSupplier = db.createFieldIndex(typeof(Shipment), "supplier", false);
            root.shipmentDetail = db.createFieldIndex(typeof(Shipment), "detail", false);
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
                        Console.Error.WriteLine("No such supplier!");
                        break;
                    }
                    detail = (Detail)root.detailId.get(new Key(input("Detail ID: ")));
                    if (detail == null) { 
                        Console.Error.WriteLine("No such detail!");
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
                    detail = (Detail)root.detailId.get(new Key(input("Detail ID: ")));
                    if (detail == null) { 
                        Console.Error.WriteLine("No such detail!");
                        break;
                    }
                    shipments = (Shipment[])root.shipmentDetail.get(new Key(detail), new Key(detail));
                    for (i = 0; i < shipments.Length; i++) { 
                        Console.WriteLine("Suppplier name: " + shipments[i].supplier.name);
                    }
                    break;
                  case 7:
                    supplier = (Supplier)root.supplierName.get(new Key(input("Supplier name: ")));
                    if (supplier == null) { 
                        Console.Error.WriteLine("No such supplier!");
                        break;
                    }
                    shipments = (Shipment[])root.shipmentSupplier.get(new Key(supplier), new Key(supplier));
                    for (i = 0; i < shipments.Length; i++) { 
                        Console.WriteLine("Detail ID: " + shipments[i].detail.id);
                    }
                    break;
                  case 8:
                    db.close();
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

