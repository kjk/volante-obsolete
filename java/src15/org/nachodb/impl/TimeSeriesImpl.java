package org.nachodb.impl;
import  org.nachodb.*;
import java.util.*;

public class TimeSeriesImpl<T extends TimeSeries.Tick> extends PersistentCollection<T> implements TimeSeries<T> { 
    public ArrayList<T> elements() { 
        return new ArrayList<T>(this);
    }

    public Object[] toArray() { 
        return elements().toArray();
    }

    public <E> E[] toArray(E[] arr) { 
        return elements().toArray(arr);
    }

    public boolean add(T tick) { 
        long time = tick.getTime();
        IPersistent[] blocks = index.get(new Key(time - maxBlockTimeInterval), new Key(time));
        if (blocks.length != 0) { 
            insertInBlock((Block)blocks[blocks.length-1], tick);
        } else { 
            addNewBlock(tick);
        }
        return true;
    }

    class TimeSeriesIterator extends IterableIterator<T> { 
        TimeSeriesIterator(long from, long till) { 
            pos = -1;
            this.till = till;
            blockIterator = index.iterator(new Key(from - maxBlockTimeInterval), new Key(till), Index.ASCENT_ORDER);
            while (blockIterator.hasNext()) { 
                Block block = (Block)blockIterator.next();
                int n = block.used;
                Tick[] e = block.getTicks();
                int l = 0, r = n;
                while (l < r)  {
                    int i = (l+r) >> 1;
                    if (from > e[i].getTime()) { 
                        l = i+1;
                    } else { 
                        r = i;
                    }
                }
                Assert.that(l == r && (l == n || e[l].getTime() >= from)); 
                if (l < n) {
                    if (e[l].getTime() <= till) { 
                        pos = l;
                        currBlock = block;
                    }
                    return;
                }
            } 
        }

        public boolean hasNext() { 
            return pos >= 0;
        }

        public T next() { 
            if (pos < 0) { 
                 throw new NoSuchElementException();
            }
            T tick = (T)currBlock.getTicks()[pos];
            if (++pos == currBlock.used) { 
                if (blockIterator.hasNext()) { 
                    currBlock = (Block)blockIterator.next();
                    pos = 0;
                } else { 
                    pos = -1;
                    return tick;
                }
            }
            if (currBlock.getTicks()[pos].getTime() > till) {
                pos = -1;
            }
            return tick;
        }

        public void remove() { 
            throw new UnsupportedOperationException();
        }

        private Iterator blockIterator;
        private Block    currBlock;
        private int      pos;
        private long     till;
    }
                
            
    class TimeSeriesReverseIterator extends IterableIterator<T> { 
        TimeSeriesReverseIterator(long from, long till) { 
            pos = -1;
            this.from = from;
            blockIterator = index.iterator(new Key(from - maxBlockTimeInterval), new Key(till), Index.DESCENT_ORDER);
            while (blockIterator.hasNext()) { 
                Block block = (Block)blockIterator.next();
                int n = block.used;
                Tick[] e =  block.getTicks();
                int l = 0, r = n;
                while (l < r)  {
                    int i = (l+r) >> 1;
                    if (till >= e[i].getTime()) { 
                        l = i+1;
                    } else { 
                        r = i;
                    }
                }
                Assert.that(l == r && (l == n || e[l].getTime() > till)); 
                if (l > 0) {
                    if (e[l-1].getTime() >= from) { 
                        pos = l-1;
                        currBlock = block;
                    }
                    return;
                }
            } 
        }

        public boolean hasNext() { 
            return pos >= 0;
        }

        public T next() { 
            if (pos < 0) { 
                 throw new NoSuchElementException();
            }
            T tick = (T)currBlock.getTicks()[pos];
            if (--pos < 0) { 
                if (blockIterator.hasNext()) { 
                    currBlock = (Block)blockIterator.next();
                    pos = currBlock.used-1;
                } else { 
                    pos = -1;
                    return tick;
                }
            }
            if (currBlock.getTicks()[pos].getTime() < from) {
                pos = -1;
            }
            return tick;
        }

        public void remove() { 
            throw new UnsupportedOperationException();
        }

        private Iterator blockIterator;
        private Block    currBlock;
        private int      pos;
        private long     from;
    }
                            
    public Iterator<T> iterator() { 
        return iterator(null, null, true);
    }

    public IterableIterator<T> iterator(Date from, Date till) {
        return iterator(from, till, true);
    }

    public IterableIterator<T> iterator(boolean ascent) {
        return iterator(null, null, ascent);
    }

    public IterableIterator<T> iterator(Date from, Date till, boolean ascent) { 
        long low = from == null ? 0 : from.getTime();
        long high = till == null ? Long.MAX_VALUE : till.getTime();
        return ascent 
            ? (IterableIterator<T>)new TimeSeriesIterator(low, high)
            : (IterableIterator<T>)new TimeSeriesReverseIterator(low, high);
    }

    public Date getFirstTime() {
        Iterator blockIterator = index.iterator();
        if (blockIterator.hasNext()) { 
            Block block = (Block)blockIterator.next();            
            return new Date(block.timestamp);
        } 
        return null;
    }

    public Date getLastTime() {
        Iterator blockIterator = index.iterator(null, null, Index.DESCENT_ORDER);
        if (blockIterator.hasNext()) { 
            Block block = (Block)blockIterator.next();            
            return new Date(block.getTicks()[block.used-1].getTime());
        } 
        return null;
    }

    public int size() {
        int n = 0;
        Iterator blockIterator = index.iterator();
        while (blockIterator.hasNext()) { 
            Block block = (Block)blockIterator.next();            
            n += block.used;
        }
        return n;
    }
       
    public T getTick(Date timestamp) {
        long time = timestamp.getTime();
        Iterator blockIterator = index.iterator(new Key(time - maxBlockTimeInterval), new Key(time), Index.ASCENT_ORDER);
        while (blockIterator.hasNext()) { 
            Block block = (Block)blockIterator.next();
            int n = block.used;
            Tick[] e = block.getTicks();
            int l = 0, r = n;
            while (l < r)  {
                int i = (l+r) >> 1;
                if (time > e[i].getTime()) { 
                    l = i+1;
                } else { 
                    r = i;
                }
            }
            Assert.that(l == r && (l == n || e[l].getTime() >= time)); 
            if (l < n && e[l].getTime() == time) { 
                return (T)e[l];
            }
        }
        return null;
    }

    public boolean has(Date timestamp) {
        return getTick(timestamp) != null;
    }

    public int remove(Date from, Date till) {
        long low = from == null ? 0 : from.getTime();
        long high = till == null ? Long.MAX_VALUE : till.getTime();
        int  nRemoved = 0;
        Key  fromKey = new Key(low - maxBlockTimeInterval);
        Key  tillKey =  new Key(high);
        Iterator blockIterator = index.iterator(fromKey, tillKey, Index.ASCENT_ORDER);
        while (blockIterator.hasNext()) { 
            Block block = (Block)blockIterator.next();
            int n = block.used;
            Tick[] e = block.getTicks();
            int l = 0, r = n;
            while (l < r)  {
                int i = (l+r) >> 1;
                if (low > e[i].getTime()) { 
                    l = i+1;
                } else { 
                    r = i;
                }
            }
            Assert.that(l == r && (l == n || e[l].getTime() >= low)); 
            while (r < n && e[r].getTime() <= high) {
                r += 1;
                nRemoved += 1;
            }
            if (l == 0 && r == n) { 
                index.remove(new Key(block.timestamp), block);
                blockIterator = index.iterator(fromKey, tillKey, Index.ASCENT_ORDER);
                block.deallocate();
            } else if (l < n && l != r) { 
                if (l == 0) { 
                    index.remove(new Key(block.timestamp), block);
                    block.timestamp = e[r].getTime();
                    index.put(new Key(block.timestamp), block);
                    blockIterator = index.iterator(fromKey, tillKey, Index.ASCENT_ORDER);
                }
                while (r < n) { 
                    e[l++] = e[r++];
                }
                block.used = l;
                block.modify();
            }
        }
        return nRemoved;
    }

    private void addNewBlock(Tick t)
    {
        Block block;
        try { 
            block = (Block)blockClass.newInstance();             
        } catch (Exception x) { 
            throw new StorageError(StorageError.CONSTRUCTOR_FAILURE, blockClass, x);
        }
        block.timestamp = t.getTime();
        block.used = 1;
        block.getTicks()[0] = t;
        index.put(new Key(block.timestamp), block);
    }

    void insertInBlock(Block block, Tick tick)
    {
        long t = tick.getTime();
        int i, n = block.used;
        
        Tick[] e =  block.getTicks();
        int l = 0, r = n;
        while (l < r)  {
            i = (l+r) >> 1;
            if (t > e[i].getTime()) { 
                l = i+1;
            } else { 
                r = i;
            }
        }
        Assert.that(l == r && (l == n || e[l].getTime() >= t));
        if (r == 0) { 
            if (e[n-1].getTime() - t > maxBlockTimeInterval || n == e.length) { 
                addNewBlock(tick);
                return;
             }
            block.timestamp = t;
        } else if (r == n) {
            if (t - e[0].getTime() > maxBlockTimeInterval || n == e.length) { 
                addNewBlock(tick);
                return;
            } 
        }
        if (n == e.length) { 
            addNewBlock(e[n-1]);
            for (i = n; --i > r; ) { 
                e[i] = e[i-1];
            }
        } else { 
            for (i = n; i > r; i--) { 
                e[i] = e[i-1];
            }
            block.used += 1;
        }
        e[r] = tick;
        block.modify();
    }

    TimeSeriesImpl(Storage storage, Class blockClass, long maxBlockTimeInterval) {
        this.blockClass = blockClass;
        this.maxBlockTimeInterval = maxBlockTimeInterval;
        blockClassName = blockClass.getName();
        index = storage.createIndex(long.class, true);
    }

    TimeSeriesImpl() {}
   
    public void onLoad() {
        blockClass = ClassDescriptor.loadClass(getStorage(), blockClassName);
    }

    public void clear() { 
        Iterator blockIterator = index.iterator();
        while (blockIterator.hasNext()) {
            Block block = (Block)blockIterator.next();
            block.deallocate();
        }
        index.clear();
    }        


    public void deallocate() {
        clear();
        index.deallocate();
        super.deallocate();
    }

    private Index index;
    private long  maxBlockTimeInterval;
    private String blockClassName;
    private transient Class blockClass;
}

