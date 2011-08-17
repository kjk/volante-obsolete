namespace Volante
{
    using System;

    public class TestTimeSeriesResult : TestResult
    {
        public TimeSpan InsertTime;
        public TimeSpan SearchTime1;
        public TimeSpan SearchTime2;
        public TimeSpan RemoveTime;
    }

    public class TestTimeSeries
    {
        public struct Quote : ITimeSeriesTick
        {
            public int timestamp;
            public float low;
            public float high;
            public float open;
            public float close;
            public int volume;

            public long Time
            {
                get
                {
                    return getTicks(timestamp);
                }
            }
        }

        public const int N_ELEMS_PER_BLOCK = 100;

        class Stock : Persistent
        {
            public string name;
            public ITimeSeries<Quote> quotes;
        }

        public void Run(TestConfig config)
        {
            Stock stock;
            int i;
            int count = config.Count;
            var res = new TestTimeSeriesResult();
            config.Result = res;

            var start = DateTime.Now;
            IStorage db = config.GetDatabase();

            IFieldIndex<string, Stock> stocks = (IFieldIndex<string, Stock>)db.Root;
            Tests.Assert(stocks == null);
            stocks = db.CreateFieldIndex<string, Stock>("name", true);
            stock = new Stock();
            stock.name = "BORL";
            stock.quotes = db.CreateTimeSeries<Quote>(N_ELEMS_PER_BLOCK, N_ELEMS_PER_BLOCK * TICKS_PER_SECOND * 2);
            stocks.Put(stock);
            db.Root = stocks;

            Random rand = new Random(2004);
            int time = getSeconds(start) - count;
            for (i = 0; i < count; i++)
            {
                Quote quote = new Quote();
                quote.timestamp = time + i;
                quote.open = (float)rand.Next(10000) / 100;
                quote.close = (float)rand.Next(10000) / 100;
                quote.high = Math.Max(quote.open, quote.close);
                quote.low = Math.Min(quote.open, quote.close);
                quote.volume = rand.Next(1000);
                stock.quotes.Add(quote);
            }
            db.Commit();
            res.InsertTime = DateTime.Now - start;

            start = DateTime.Now;
            rand = new Random(2004);
            start = DateTime.Now;
            i = 0;
            foreach (Quote quote in stock.quotes)
            {
                Tests.Assert(quote.timestamp == time + i);
                float open = (float)rand.Next(10000) / 100;
                Tests.Assert(quote.open == open);
                float close = (float)rand.Next(10000) / 100;
                Tests.Assert(quote.close == close);
                Tests.Assert(quote.high == Math.Max(quote.open, quote.close));
                Tests.Assert(quote.low == Math.Min(quote.open, quote.close));
                Tests.Assert(quote.volume == rand.Next(1000));
                i += 1;
            }
            Tests.Assert(i == count);
            res.SearchTime1 = DateTime.Now - start;

            start = DateTime.Now;
            Tests.Assert(stock.quotes.Count == count);
            long from = getTicks(time + 1000);
            int count2 = 1000;
            start = DateTime.Now;
            i = 0;
            foreach (Quote quote in stock.quotes.Range(new DateTime(from), new DateTime(from + count2 * TICKS_PER_SECOND), IterationOrder.DescentOrder))
            {
                Tests.Assert(quote.timestamp == time + 1000 + count2 - i);
                i += 1;
            }
            Tests.Assert(i == count2 + 1);
            res.SearchTime2 = DateTime.Now - start;
            start = DateTime.Now;

            long n = stock.quotes.Remove(stock.quotes.FirstTime, stock.quotes.LastTime);
            Tests.Assert(n == count);
            res.RemoveTime = DateTime.Now - start;
            Tests.Assert(stock.quotes.Count == 0);
            db.Close();
        }

        const long TICKS_PER_SECOND = 10000000L;

        static DateTime baseDate = new DateTime(1970, 1, 1);

        static int getSeconds(DateTime dt)
        {
            return (int)((dt.Ticks - baseDate.Ticks) / TICKS_PER_SECOND);
        }

        static long getTicks(int seconds)
        {
            return baseDate.Ticks + seconds * TICKS_PER_SECOND;
        }

    }

}
