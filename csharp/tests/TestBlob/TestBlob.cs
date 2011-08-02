using System;
using Volante;

public class TestBlobRunner
{
    public static void Main(string[] args)
    {
        Tests.SafeDeleteFile(TestBlob.dbName);
        TestBlob.Run();
        TestBlob.Run();
    }
}
