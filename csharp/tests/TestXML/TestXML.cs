using System;
using NachoDB;

public class TestXMLRunner
{
    static public void  Main(System.String[] args)
    {
#if !OMIT_XML
        TestXml.Run(100000, false);
#endif
    }
}
