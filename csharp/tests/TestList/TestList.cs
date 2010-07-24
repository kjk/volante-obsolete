using System;
using NachoDB;

public class TestListRunner
{
    static void Main(string[] args)
    {
        int n = 10 * 1000 * 1000;
        for (int i = 0; i < args.Length; i++) 
        {
            Int32.TryParse(args[i], out n);
        }
        TestList.Run(n);
    }
}

