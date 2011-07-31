using System;
using Volante;

public class TestEnumeratorRunner
{
    static public void Main(string[] args) 
    {
        int n = 1000;
        for (int i = 0; i < args.Length; i++) 
        {
            Int32.TryParse(args[i], out n);
        }

        TestEnumerator.Run(n, false);
        TestEnumerator.Run(n, true);
    }
}

