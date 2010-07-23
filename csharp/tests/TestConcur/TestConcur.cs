using System;
using NachoDB;

public class TestConcurRunner
{
    public static void Main(String[] args) 
    {
        TestConcur.Run(100000);
    }
}

