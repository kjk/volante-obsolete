using System;
using NachoDB;

public class TestGcRunner
{
    static public void Main(String[] args)
    {
        bool altBtree = false;
        bool backgroundGc = false;
        int iterations = 100000;

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
            else if (Int32.TryParse(args[i], out iterations))
            {
                // do nothing
            }
            else
            {
                Console.WriteLine("Unrecognized option: " + args[i]);
            }
        }
        TestGC.Run(iterations, altBtree, backgroundGc);
    }
}

