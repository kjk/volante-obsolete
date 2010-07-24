using System;
using NachoDB;

public class TestR2Runner
{
    public static void Main(String[] args) 
    {
        int n = 100000;
        for (int i = 0; i < args.Length; i++) 
        {
            Int32.TryParse(args[i], out n);
        }

        TestR2.Run(n, false);
        TestR2.Run(n, true);
    }
}

