using System;
using NachoDB;

public class TestEnumeratorRunner
{
    static public void Main(string[] args) 
    {
        TestEnumerator.Run(1000, false);
        TestEnumerator.Run(1000, true);
    }
}

