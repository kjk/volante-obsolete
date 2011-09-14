namespace Volante
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public class TestTimeSeriesResult : TestResult
    {
        public TimeSpan InsertTime;
        public TimeSpan SearchTime1;
        public TimeSpan SearchTime2;
        public TimeSpan RemoveTime;
    }

    public class TestTimeSeries : ITest
    {
        public struct Quote : ITimeSeriesTick
        {
            public int timestamp;
            public float low;
            public float high;
            public float open;
            public float close;
            public int volume;

            public long Ticks
            {
                get
                {
                    return getTicks(timestamp);
                }
            }
        }

        public static Random rand;

        public static Quote NewQuote(int timestamp)
        {
            Quote quote = new Quote();
            quote.timestamp = timestamp;
            quote.open = (float)rand.Next(10000) / 100;
            quote.close = (float)rand.Next(10000) / 100;
            quote.high = Math.Max(quote.open, quote.close);
            quote.low = Math.Min(quote.open, quote.close);
            quote.volume = rand.Next(1000);
            return quote;
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
            IDatabase db = config.GetDatabase();

            IFieldIndex<string, Stock> stocks = (IFieldIndex<string, Stock>)db.Root;
            Tests.Assert(stocks == null);
            stocks = db.CreateFieldIndex<string, Stock>("name", IndexType.Unique);
            stock = new Stock();
            stock.name = "BORL";
            stock.quotes = db.CreateTimeSeries<Quote>(N_ELEMS_PER_BLOCK, N_ELEMS_PER_BLOCK * TICKS_PER_SECOND * 2);
            stocks.Put(stock);
            db.Root = stocks;

            Tests.Assert(!stock.quotes.IsReadOnly);
            rand = new Random(2004);
            int startTimeInSecs = getSeconds(start);
            int currTime = startTimeInSecs;
            for (i = 0; i < count; i++)
            {
                Quote quote = NewQuote(currTime++);
                stock.quotes.Add(quote);
            }
            Tests.Assert(stock.quotes.Count == count);
            db.Commit();
            Tests.Assert(stock.quotes.Count == count);
            res.InsertTime = DateTime.Now - start;

            start = DateTime.Now;
            rand = new Random(2004);
            start = DateTime.Now;
            i = 0;
            foreach (Quote quote in stock.quotes)
            {
                Tests.Assert(quote.timestamp == startTimeInSecs + i);
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
            long from = getTicks(startTimeInSecs + count / 2);
            long till = getTicks(startTimeInSecs + count);
            i = 0;
            foreach (Quote quote in stock.quotes.Range(new DateTime(from), new DateTime(till), IterationOrder.DescentOrder))
            {
                int expectedtimestamp = startTimeInSecs + count - i - 1;
                Tests.Assert(quote.timestamp == expectedtimestamp);
                i += 1;
            }
            res.SearchTime2 = DateTime.Now - start;
            start = DateTime.Now;

            // insert in the middle
            stock.quotes.Add(NewQuote(startTimeInSecs - count / 2));

            long n = stock.quotes.Remove(stock.quotes.FirstTime, stock.quotes.LastTime);
            Tests.Assert(n == count + 1);
            Tests.Assert(stock.quotes.Count == 0);
            res.RemoveTime = DateTime.Now - start;

            Quote q;
            Quote qFirst = NewQuote(0);
            Quote qMiddle = NewQuote(0);
            Quote qEnd = NewQuote(0);
            for (i = 0; i < 10; i++)
            {
                q = NewQuote(startTimeInSecs + i);
                stock.quotes.Add(q);
                if (i == 0)
                    qFirst = q;
                else if (i == 5)
                    qMiddle = q;
                else if (i == 9)
                    qEnd = q;
            }
            Tests.Assert(stock.quotes.Contains(qFirst));
            Tests.Assert(stock.quotes.Contains(qEnd));
            Tests.Assert(stock.quotes.Contains(qMiddle));
            Tests.Assert(stock.quotes.Remove(qFirst));
            Tests.Assert(!stock.quotes.Contains(qFirst));
            Tests.Assert(stock.quotes.Remove(qEnd));
            Tests.Assert(!stock.quotes.Contains(qEnd));
            Tests.Assert(stock.quotes.Remove(qMiddle));
            Tests.Assert(!stock.quotes.Contains(qMiddle));

            Quote[] quotes = new Quote[10];
            stock.quotes.CopyTo(quotes, 0);
            stock.quotes.Clear();

            Tests.AssertDatabaseException(
                () => { long tmp = stock.quotes.FirstTime.Ticks; }, DatabaseException.ErrorCode.KEY_NOT_FOUND);
            Tests.AssertDatabaseException(
                () => { long tmp = stock.quotes.LastTime.Ticks; }, DatabaseException.ErrorCode.KEY_NOT_FOUND);

            for (i = 0; i < 10; i++)
            {
                q = NewQuote(startTimeInSecs + i);
                stock.quotes.Add(q);
            }

            IEnumerator e = stock.quotes.GetEnumerator();
            i = 0;
            while (e.MoveNext())
            {
                i++;
            }
            Tests.Assert(i == 10);
            Tests.Assert(!e.MoveNext());
            Tests.AssertException<InvalidOperationException>(
                () => { object o = e.Current; });
            e.Reset();
            Tests.Assert(e.MoveNext());

            e = stock.quotes.Reverse().GetEnumerator();
            i = 0;
            while (e.MoveNext())
            {
                i++;
            }
            Tests.Assert(i == 10);
            Tests.Assert(!e.MoveNext());
            Tests.AssertException<InvalidOperationException>(
                () => { object o = e.Current; });
            e.Reset();
            Tests.Assert(e.MoveNext());
            DateTime tStart = new DateTime(getTicks(startTimeInSecs));
            DateTime tMiddle = new DateTime(getTicks(startTimeInSecs+5));
            DateTime tEnd = new DateTime(getTicks(startTimeInSecs+9));

            IEnumerator<Quote> e2 = stock.quotes.GetEnumerator(tStart, tMiddle);
            VerifyEnumerator(e2, tStart.Ticks, tMiddle.Ticks);
            e2 = stock.quotes.GetEnumerator(tStart, tMiddle, IterationOrder.DescentOrder);
            VerifyEnumerator(e2, tStart.Ticks, tMiddle.Ticks, IterationOrder.DescentOrder);

            e2 = stock.quotes.GetEnumerator(IterationOrder.DescentOrder);
            VerifyEnumerator(e2, tStart.Ticks, tEnd.Ticks, IterationOrder.DescentOrder);

            e2 = stock.quotes.Range(tMiddle, tEnd, IterationOrder.AscentOrder).GetEnumerator();
            VerifyEnumerator(e2, tMiddle.Ticks, tEnd.Ticks, IterationOrder.AscentOrder);

            e2 = stock.quotes.Range(IterationOrder.DescentOrder).GetEnumerator();
            VerifyEnumerator(e2, tStart.Ticks, tEnd.Ticks, IterationOrder.DescentOrder);

            e2 = stock.quotes.Till(tMiddle).GetEnumerator();
            VerifyEnumerator(e2, tStart.Ticks, tMiddle.Ticks, IterationOrder.DescentOrder);

            e2 = stock.quotes.From(tMiddle).GetEnumerator();
            VerifyEnumerator(e2, tMiddle.Ticks, tEnd.Ticks);

            e2 = stock.quotes.Reverse().GetEnumerator();
            VerifyEnumerator(e2, tStart.Ticks, tEnd.Ticks, IterationOrder.DescentOrder);

            Tests.Assert(stock.quotes.FirstTime.Ticks == tStart.Ticks);
            Tests.Assert(stock.quotes.LastTime.Ticks == tEnd.Ticks);
            for (i = 0; i < 10; i++)
            {
                long ticks = getTicks(startTimeInSecs + i);
                Quote qTmp = stock.quotes[new DateTime(ticks)];
                Tests.Assert(qTmp.Ticks == ticks);
                Tests.Assert(stock.quotes.Contains(new DateTime(ticks)));
            }
            Tests.Assert(!stock.quotes.Contains(new DateTime(0)));

            Tests.AssertDatabaseException(
                () => { Quote tmp = stock.quotes[new DateTime(0)]; }, DatabaseException.ErrorCode.KEY_NOT_FOUND);

            stock.quotes.RemoveFrom(new DateTime(getTicks(startTimeInSecs + 8)));
            stock.quotes.RemoveFrom(new DateTime(getTicks(startTimeInSecs + 2)));
            stock.quotes.RemoveAll();
            db.Commit();
            stock.quotes.Deallocate();
            db.Commit();
            db.Close();
        }

        void VerifyEnumerator(IEnumerator<Quote> e, long tickStart, long tickEnd, IterationOrder order = IterationOrder.AscentOrder)
        {
            int n = 0;
            long tickCurr = 0;
            if (order == IterationOrder.DescentOrder)
                tickCurr = long.MaxValue;
            while (e.MoveNext())
            {
                Quote q = e.Current;
                Tests.Assert(q.Ticks >= tickStart);
                Tests.Assert(q.Ticks <= tickEnd);
                // TODO: FAILED_TEST
                if (order == IterationOrder.AscentOrder)
                    Tests.Assert(q.Ticks >= tickCurr);
                else
                    Tests.Assert(q.Ticks <= tickCurr);
                tickCurr = q.Ticks;
                n++;
            }
            Tests.Assert(!e.MoveNext());
            Tests.AssertException<InvalidOperationException>(
                () => { long ticks = e.Current.Ticks; });
            e.Reset();
            Tests.Assert(e.MoveNext());
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
