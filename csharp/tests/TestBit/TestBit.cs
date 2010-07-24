using System;
using NachoDB;

public class TestBitRunner
{ 
    static public void Main(string[] args) 
    {
        int n = 1000000;
        for (int i = 0; i < args.Length; i++) 
        {
            Int32.TryParse(args[i], out n);
        }

        TestBit.Run(n);
    }
}
