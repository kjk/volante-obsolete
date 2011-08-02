using System;
using Volante;

public class TestRawRunner
{
    public static void Main(String[] args) 
    {
        Tests.SafeDeleteFile(TestRaw.dbName);
        TestRaw.Run();
        TestRaw.Run();
    }
}
