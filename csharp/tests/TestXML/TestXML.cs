using System;
using NachoDB;

public class TestXMLRunner
{
    static public void  Main(System.String[] args)
    {
#if !OMIT_XML
        int n = 100000;
        for (int i = 0; i < args.Length; i++) 
        {
            Int32.TryParse(args[i], out n);
        }

        TestXml.Run(n, false);
#else
        Console.WriteLine("XML code not available in this build");
#endif
    }
}
