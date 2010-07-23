using System;
using NachoDB;

public class TestR2Runner
{
    public static void Main(String[] args) 
    {
        TestR2.Run(100000, false);
        TestR2.Run(100000, true);
    }
}

