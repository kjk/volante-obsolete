namespace Volante
{
    using System;

    // TODO: finish me
    public class TestFieldIndex : ITest
    {
        class Record : Persistent
        {
            internal String strKey;
            internal int intKey;
        }

        class Root : Persistent
        {

        }

        public void Run(TestConfig config)
        {
            int count = config.Count;
            var res = new TestResult();
            config.Result = res;

        }
    }
}
