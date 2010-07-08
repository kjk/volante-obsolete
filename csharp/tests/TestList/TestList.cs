using System;
using NachoDB;

public abstract class LinkNode: Persistent
{
    public abstract int Number
    {
        get;set;
    }

    public abstract LinkNode Next
    {
        get;set;
    }
}

public class TestList
{
    static void Main(string[] args)
    {
        Storage db = StorageFactory.CreateStorage();

        db.Open("LinkedList.dbs", 10 * 1024 * 1024, "LinkedList"); // 10M cache

        db.Root = db.CreateClass(typeof(LinkNode));
        LinkNode header = (LinkNode)db.Root;
        LinkNode current;

        /****************************** insert section *******************************/

        current = header;
        // Now I will insert 10 million node objects to the list tail
        int totalnumber = 10 * 1000 * 1000;
        DateTime t1 = DateTime.Now;
        for (int i = 0; i < totalnumber; i++)
        {
            if (i % 10000 == 0)
                Console.Write("\r" + (i * 100L / totalnumber).ToString() + "% finished");
            current.Next = (LinkNode)db.CreateClass(typeof(LinkNode));
            current = current.Next;
            current.Number = i;
        }
        DateTime t2 = DateTime.Now;
        Console.WriteLine("\r Insert Time: " + (t2 - t1).TotalSeconds);

        /****************************** traverse read ********************************/

        int number = 0; // A variable used to validate the data in list
        current = header;
        t1 = DateTime.Now;
        while (current.Next != null) // Traverse the whole list in the database
        {
            if (number % 10000 == 0)
                Console.Write("\r" + (number * 100L / totalnumber).ToString() + "% finished");
            current = current.Next;
            if (current.Number != number++) // Validate node's value
                throw new Exception("error number");
        }
        t2 = DateTime.Now;
        Console.WriteLine("\r Traverse Read Time: " + (t2 - t1).TotalSeconds);
        Console.WriteLine("TotalNumber = " + number);

        /****************************** traverse modify *******************************/

        number = 0;
        current = header;
        t1 = DateTime.Now;
        while (current.Next != null) // Traverse the whole list in the database
        {
            if (number % 10000 == 0)
                Console.Write("\r" + (number * 100L / totalnumber).ToString() + "% finished");
            current = current.Next;
            if (current.Number != number++) // Validate node's value
                throw new Exception("error number");
            current.Number = -current.Number;
        }
        t2 = DateTime.Now;
        Console.WriteLine("\r Traverse Modify Time: " + (t2 - t1).TotalSeconds);
        Console.WriteLine("TotalNumber = " + number);

        db.Close();
    }
}


