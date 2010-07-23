using System;
using NachoDB;

public class TestListRunner
{
    static void Main(string[] args)
    {
        TestList.Run(10 * 1000 * 1000);
    }
}

