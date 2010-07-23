using System;
using NachoDB;

public class TestCompoundIndexRunner
{
    static public void Main(string[] args) 
    {
        bool altBtree = false;
        for (int i = 0; i < args.Length; i++) 
        { 
            if ("altbtree" == args[i]) 
            {
                altBtree = true;
            }
        }
        TestCompoundIndex.Run(altBtree, 100000);
    }
}
