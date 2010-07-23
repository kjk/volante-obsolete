using System;
using NachoDB;

public class TestGcRunner
{
    static public void Main(String[] args)
    {
        bool altBtree = false;
        bool backgroundGc = false;

        for (int i = 0; i < args.Length; i++) 
        { 
            if ("altbtree" == args[i]) 
            {
                altBtree = true;
            } 
            else if ("background" == args[i]) 
            {
                backgroundGc = true;
            } 
            else 
            { 
                Console.WriteLine("Unrecognized option: " + args[i]);
            }
        }
        TestGC.Run(100000, altBtree, backgroundGc);
    }
}

