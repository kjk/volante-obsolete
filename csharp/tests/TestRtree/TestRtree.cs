using System;
using NachoDB;

public class TestRtreeRunner
{
    public static void Main(String[] args)
    {
        int n = 100000;
        for (int i = 0; i < args.Length; i++) 
        {
            Int32.TryParse(args[i], out n);
        }

        TestRtree.Run(n);
    }
}

