using System;
using NachoDB;

public class TestIndexRunner
{
    static public void Main(string[] args)
    {
        bool inmemory = false;
        bool altBtree = false;
        bool serializableTransaction = false;

        for (int i = 0; i < args.Length; i++) 
        { 
            if ("inmemory" == args[i]) 
            {
                inmemory = true;
            } 
            else if ("altbtree" == args[i]) 
            {
                altBtree = true;
            } 
            else if ("serializable" == args[i]) 
            { 
                serializableTransaction = true;
            } 
            else 
            { 
                Console.WriteLine("Unrecognized option: " + args[i]);
            }
        }

        TestIndex.Run(100000, altBtree, inmemory, serializableTransaction);
    }    
}
    