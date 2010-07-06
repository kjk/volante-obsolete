using System;
using Perst;


public class TestTimeSeries { 
    public struct Quote : TimeSeriesTick 
    { 
        public int   timestamp;
        public float low;
        public float high;
        public float open;
        public float close;
        public int   volume;

        public long Time 
        { 
            get 
            { 
                return getTicks(timestamp);
            }
        }
    }
    
    public class QuoteBlock : TimeSeriesBlock 
    {
        private Quote[] quotes;
        
        public const int N_ELEMS_PER_BLOCK = 100;

        public override TimeSeriesTick this[int i] 
        {
            get 
            {
                return quotes[i];
            }
            set 
            {
                quotes[i] = (Quote)value;
            }
        }

        public override Array Ticks 
        {
            get 
            {
                return quotes;
            }
        }

        public QuoteBlock() 
        {
            quotes = new Quote[N_ELEMS_PER_BLOCK];
        }
    }

    class Stock : Persistent { 
        public string     name;
        public TimeSeries quotes;
    }

    const int nElements = 1000000;
    const int pagePoolSize = 32*1024*1024;

    static public void Main(string[] args) {   
        Stock stock;
        int i;

        Storage db = StorageFactory.Instance.CreateStorage();
        db.Open("testts.dbs", pagePoolSize);
        FieldIndex stocks = (FieldIndex)db.Root;
        if (stocks == null) { 
            stocks = db.CreateFieldIndex(typeof(Stock), "name", true);
            stock = new Stock();
            stock.name = "BORL";
            stock.quotes = db.CreateTimeSeries(typeof(QuoteBlock), QuoteBlock.N_ELEMS_PER_BLOCK*TICKS_PER_SECOND*2);
            stocks.Put(stock);
            db.Root = stocks;
        } else { 
            stock = (Stock)stocks["BORL"];
        }
        Random rand = new Random(2004);
        DateTime start = DateTime.Now;
        int time = getSeconds(start) - nElements;
        for (i = 0; i < nElements; i++) { 
            Quote quote = new Quote();        
            quote.timestamp = time + i;
            quote.open = (float)rand.Next(10000)/100;
            quote.close = (float)rand.Next(10000)/100;
            quote.high = Math.Max(quote.open, quote.close);
            quote.low = Math.Min(quote.open, quote.close);
            quote.volume = rand.Next(1000);
            stock.quotes.Add(quote);
        }
        db.Commit();
        Console.WriteLine("Elapsed time for storing " + nElements + " quotes: " 
                          + (DateTime.Now - start));
        
        rand = new Random(2004);
        start = DateTime.Now;
        i = 0;
        foreach (Quote quote in stock.quotes) 
        {
            Assert.That(quote.timestamp == time + i);
            float open = (float)rand.Next(10000)/100;
            Assert.That(quote.open == open);
            float close = (float)rand.Next(10000)/100;
            Assert.That(quote.close == close);
            Assert.That(quote.high == Math.Max(quote.open, quote.close));
            Assert.That(quote.low == Math.Min(quote.open, quote.close));
            Assert.That(quote.volume == rand.Next(1000));
            i += 1;
        }
        Assert.That(i == nElements);
        Console.WriteLine("Elapsed time for extracting " + nElements + " quotes: " 
                           + (DateTime.Now - start));
                 
        Assert.That(stock.quotes.Count == nElements);
        
        
        long from = getTicks(time+1000);
        int count = 1000;
        start = DateTime.Now;
        i = 0;
        foreach (Quote quote in stock.quotes.Range(new DateTime(from), new DateTime(from + count*TICKS_PER_SECOND), IterationOrder.DescentOrder)) {
            Assert.That(quote.timestamp == time + 1000 + count - i);
            i += 1;
        }
        Assert.That(i == count+1);
        Console.WriteLine("Elapsed time for extracting " + i + " quotes: " + (DateTime.Now - start));

        start = DateTime.Now;
        long n = stock.quotes.Remove(stock.quotes.FirstTime, stock.quotes.LastTime);
        Assert.That(n == nElements);
        Console.WriteLine("Elapsed time for removing " + nElements + " quotes: " 
                           + (DateTime.Now - start));

        Assert.That(stock.quotes.Count == 0);
        
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
