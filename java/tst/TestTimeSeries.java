import org.garret.perst.*;

import java.util.*;

public class TestTimeSeries { 
    public static class Quote implements TimeSeriesTick { 
        int   timestamp;
        float low;
        float high;
        float open;
        float close;
        int   volume;

        public long getTime() { 
            return (long)timestamp*1000;
        }
    }
    
    public static class QuoteBlock extends TimeSeriesBlock {
        private Quote[] quotes;
        
        static final int N_ELEMS_PER_BLOCK = 100;

        public TimeSeriesTick[] getTicks() { 
            if (quotes == null) { 
                quotes = new Quote[N_ELEMS_PER_BLOCK];
                for (int i = 0; i < N_ELEMS_PER_BLOCK; i++) { 
                    quotes[i] = new Quote();
                }
            }
            return quotes;
        }
    }

    static class Stock extends Persistent { 
        String     name;
        TimeSeries quotes;
    }

    final static int nElements = 10000000;
    final static int pagePoolSize = 32*1024*1024;

    static public void main(String[] args) throws Exception {   
        Stock stock;
        int i;

        Storage db = StorageFactory.getInstance().createStorage();
        db.open("testts.dbs", pagePoolSize);
        FieldIndex stocks = (FieldIndex)db.getRoot();
        if (stocks == null) { 
            stocks = db.createFieldIndex(Stock.class, "name", true);
            stock = new Stock();
            stock.name = "BORL";
            stock.quotes = db.createTimeSeries(QuoteBlock.class, (long)QuoteBlock.N_ELEMS_PER_BLOCK*1000*2);
            stocks.put(stock);
            db.setRoot(stocks);
        } else { 
            stock = (Stock)stocks.get("BORL");
        }
        Random rand = new Random(2004);
        long start = System.currentTimeMillis();
        int time = (int)(start/1000) - nElements;
        for (i = 0; i < nElements; i++) { 
            Quote quote = new Quote();        
            quote.timestamp = time + i;
            quote.open = (float)rand.nextInt(10000)/100;
            quote.close = (float)rand.nextInt(10000)/100;
            quote.high = Math.max(quote.open, quote.close);
            quote.low = Math.min(quote.open, quote.close);
            quote.volume = rand.nextInt(1000);
            stock.quotes.add(quote);
        }
        db.commit();
        System.out.println("Elapsed time for storing " + nElements + " quotes: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
        
        rand.setSeed(2004);
        start = System.currentTimeMillis();
        Iterator iterator = stock.quotes.iterator();
        for (i = 0; iterator.hasNext(); i++) { 
            Quote quote = (Quote)iterator.next();
            Assert.that(quote.timestamp == time + i);
            float open = (float)rand.nextInt(10000)/100;
            Assert.that(quote.open == open);
            float close = (float)rand.nextInt(10000)/100;
            Assert.that(quote.close == close);
            Assert.that(quote.high == Math.max(quote.open, quote.close));
            Assert.that(quote.low == Math.min(quote.open, quote.close));
            Assert.that(quote.volume == rand.nextInt(1000));
        }
        Assert.that(i == nElements);
        System.out.println("Elapsed time for extracting " + nElements + " quotes: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");
                 
        Assert.that(stock.quotes.size() == nElements);
        
        
        long from = (long)(time+1000)*1000;
        int count = 1000;
        start = System.currentTimeMillis();
        iterator = stock.quotes.iterator(new Date(from), new Date(from + count*1000), false);
        for (i = 0; iterator.hasNext(); i++) { 
            Quote quote = (Quote)iterator.next();
            Assert.that(quote.timestamp == time + 1000 + count - i);
        }
        Assert.that(i == count+1);
        System.out.println("Elapsed time for extracting " + i + " quotes: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        start = System.currentTimeMillis();
        long n = stock.quotes.remove(stock.quotes.getFirstTime(), stock.quotes.getLastTime());
        Assert.that(n == nElements);
        System.out.println("Elapsed time for removing " + nElements + " quotes: " 
                           + (System.currentTimeMillis() - start) + " milliseconds");

        Assert.that(stock.quotes.size() == 0);
        
        db.close();
    }
}




