namespace Perst.Impl
{
    using System;
    using System.Collections;
    using System.Reflection;
    using Perst;
	
    public class TimeSeriesImpl : PersistentResource, TimeSeries 
    { 
        public void Add(TimeSeriesTick tick) 
        { 
            long time = tick.Time;
            foreach (TimeSeriesBlock block in index.Range(time - maxBlockTimeInterval, time, IterationOrder.DescentOrder)) {
                insertInBlock(block, tick);
                return;
            } 
            addNewBlock(tick);
        }

        class TimeSeriesEnumerator : IEnumerator, IEnumerable 
        { 
            internal TimeSeriesEnumerator(IEnumerator blockIterator, long from, long till) 
            { 
                this.till = till;
                this.from = from;
                this.blockIterator = blockIterator;
                Reset();
            }

            public IEnumerator GetEnumerator() 
            { 
                return this;
            }
        
            public void Reset()
            {
                if (resetNeeded) 
                {
                    blockIterator.Reset();
                }
                resetNeeded = true;
                hasCurrent = false;
                pos = -1;
                while (blockIterator.MoveNext()) 
                {
                    TimeSeriesBlock block = (TimeSeriesBlock)blockIterator.Current;
                    int n = block.used;
                    int l = 0, r = n;
                    while (l < r)  
                    {
                        int i = (l+r) >> 1;
                        if (from > block[i].Time) 
                        { 
                            l = i+1;
                        } 
                        else 
                        { 
                            r = i;
                        }
                    }
                    Assert.That(l == r && (l == n || block[l].Time >= from)); 
                    if (l < n) 
                    {
                        pos = l;
                        currBlock = block;
                        return;
                    }
                } 
            }

            public bool MoveNext() 
            {
                if (hasCurrent) 
                { 
                    hasCurrent = false;
                    if (++pos == currBlock.used) 
                    { 
                        if (blockIterator.MoveNext()) 
                        { 
                            currBlock = (TimeSeriesBlock)blockIterator.Current;
                            pos = 0;
                        } 
                        else 
                        { 
                            pos = -1;
                            return false;
                        }
                    }
                } 
                else if (pos < 0) 
                { 
                    return false;
                }
                if (currBlock[pos].Time > till) 
                {
                    pos = -1;
                    return false;
                } 
                hasCurrent = true;
                return true;
            }

            public virtual object Current 
            {
                get 
                {
                    if (!hasCurrent) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return currBlock[pos];
                }
            }

            private IEnumerator     blockIterator;
            private bool            hasCurrent;
            private bool            resetNeeded;
            private TimeSeriesBlock currBlock;
            private int             pos;
            private long            from;
            private long            till;
        }
                
            
        class TimeSeriesReverseEnumerator : IEnumerator, IEnumerable
        { 
            internal TimeSeriesReverseEnumerator(IEnumerator blockIterator, long from, long till) 
            { 
                this.till = till;
                this.from = from;
                this.blockIterator = blockIterator;
                Reset();
            }

            public IEnumerator GetEnumerator() 
            { 
                return this;
            }

            public void Reset()
            {
                if (resetNeeded) 
                {
                    blockIterator.Reset();
                }
                resetNeeded = true;
                hasCurrent = false;
                pos = -1;
                while (blockIterator.MoveNext()) 
                {
                    TimeSeriesBlock block = (TimeSeriesBlock)blockIterator.Current;
                    int n = block.used;
                    int l = 0, r = n;
                    while (l < r)  
                    {
                        int i = (l+r) >> 1;
                        if (till >= block[i].Time) 
                        { 
                            l = i+1;
                        } 
                        else 
                        { 
                            r = i;
                        }
                    }
                    Assert.That(l == r && (l == n || block[l].Time > till)); 
                    if (l > 0) 
                    {
                        pos = l-1;
                        currBlock = block;
                        return;
                    }
                } 
            }

            public bool MoveNext() 
            {
                if (hasCurrent) 
                { 
                    hasCurrent = false;
                    if (--pos < 0) 
                    { 
                        if (blockIterator.MoveNext()) 
                        { 
                            currBlock = (TimeSeriesBlock)blockIterator.Current;
                            pos = currBlock.used-1;
                        } 
                        else 
                        { 
                            pos = -1;
                            return false;
                        }
                    }
                } 
                else if (pos < 0) 
                { 
                    return false;
                }
                if (currBlock[pos].Time < from) 
                {
                    pos = -1;
                    return false;
                } 
                hasCurrent = true;
                return true;
            }

            public virtual object Current 
            {
                get 
                {
                    if (!hasCurrent) 
                    { 
                        throw new InvalidOperationException();
                    }
                    return currBlock[pos];
                }
            }

            private IEnumerator     blockIterator;
            private bool            hasCurrent;
            private bool            resetNeeded;
            private TimeSeriesBlock currBlock;
            private int             pos;
            private long            from;
            private long            till;
        }
                
                            
        public IEnumerator GetEnumerator() 
        { 
            return iterator(0, Int64.MaxValue, IterationOrder.AscentOrder).GetEnumerator();
        }

        public IEnumerator GetEnumerator(DateTime from, DateTime till) 
        {
            return iterator(from.Ticks, till.Ticks, IterationOrder.AscentOrder).GetEnumerator();
        }

        public IEnumerator GetEnumerator(DateTime from, DateTime till, IterationOrder order) 
        {
            return iterator(from.Ticks, till.Ticks, order).GetEnumerator();
        }

        public IEnumerator GetEnumerator(IterationOrder order) 
        {
            return iterator(0, Int64.MaxValue, order).GetEnumerator();
        }

        public IEnumerable Range(DateTime from, DateTime till) 
        {
            return iterator(from.Ticks, till.Ticks, IterationOrder.AscentOrder);
        }

        public IEnumerable Range(DateTime from, DateTime till, IterationOrder order) 
        {
            return iterator(from.Ticks, till.Ticks, order);
        }

        public IEnumerable Range(IterationOrder order) 
        {
            return iterator(0, Int64.MaxValue, order);
        }

        public IEnumerable Till(DateTime till) 
        { 
            return iterator(0, till.Ticks, IterationOrder.DescentOrder);
        }

        public IEnumerable From(DateTime from) 
        { 
            return iterator(from.Ticks, Int64.MaxValue, IterationOrder.AscentOrder);
        }

        public IEnumerable Reverse() 
        { 
            return iterator(0, Int64.MaxValue, IterationOrder.DescentOrder);
        }


        private IEnumerable iterator(long from, long till, IterationOrder order) 
        { 
            IEnumerator enumerator = index.GetEnumerator(from - maxBlockTimeInterval, till, order);
            return order == IterationOrder.AscentOrder
                ? (IEnumerable)new TimeSeriesEnumerator(enumerator, from, till)
                : (IEnumerable)new TimeSeriesReverseEnumerator(enumerator, from, till);
        }

        public DateTime FirstTime 
        {
            get 
            { 
                foreach (TimeSeriesBlock block in index) 
                {
                    return new DateTime(block.timestamp);
                } 
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
            }
        }

        public DateTime LastTime 
        {
            get 
            {
                foreach (TimeSeriesBlock block in index.Range(null, null, IterationOrder.DescentOrder)) 
                {
                    return new DateTime(block[block.used-1].Time);
                } 
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
            }
        }

        public long Count 
        {
            get 
            {
                long n = 0;
                foreach (TimeSeriesBlock block in index) 
                {
                    n += block.used;
                }
                return n;
            }
        }
       
        public TimeSeriesTick this[DateTime timestamp]     
        {
            get 
            {
                long time = timestamp.Ticks;
                foreach (TimeSeriesBlock block in index) 
                {
                    int n = block.used;
                    int l = 0, r = n;
                    while (l < r)  
                    {
                        int i = (l+r) >> 1;
                        if (time > block[i].Time) 
                        { 
                            l = i+1;
                        } 
                        else 
                        { 
                            r = i;
                        }
                    }
                    Assert.That(l == r && (l == n || block[l].Time >= time)); 
                    if (l < n && block[l].Time == time) 
                    { 
                        return block[l];
                    }
                }
                return null;
            }
        }

        public bool Contains(DateTime timestamp) 
        {
            return this[timestamp] != null;
        }

        public long Remove(DateTime from, DateTime till) 
        {
            return remove(from.Ticks, till.Ticks);
        }

        public long RemoveFrom(DateTime from) 
        {
            return remove(from.Ticks, Int64.MaxValue);
        }

        public long RemoveTill(DateTime till) 
        {
            return remove(0, till.Ticks);
        }

        public long RemoveAll() 
        {
            return remove(0, Int64.MaxValue);
        }

        private long remove(long from, long till)
        {
            long nRemoved = 0;
            IEnumerator blockIterator = index.GetEnumerator(from - maxBlockTimeInterval, till);
            while (blockIterator.MoveNext()) 
            {
                TimeSeriesBlock block = (TimeSeriesBlock)blockIterator.Current;
                int n = block.used;
                int l = 0, r = n;
                while (l < r)  
                {
                    int i = (l+r) >> 1;
                    if (from > block[i].Time) 
                    { 
                        l = i+1;
                    } 
                    else 
                    { 
                        r = i;
                    }
                }
                Assert.That(l == r && (l == n || block[l].Time >= from)); 
                while (r < n && block[r].Time <= till) 
                {
                    r += 1;
                    nRemoved += 1;
                }
                if (l == 0 && r == n) 
                { 
                    index.Remove(block.timestamp, block);
                    block.Deallocate();
                    blockIterator.Reset();
                } 
                else if (l != r) 
                { 
                    if (l == 0) 
                    { 
                        index.Remove(block.timestamp, block);
                        block.timestamp = block[r].Time;
                        index.Put(block.timestamp, block);
                        blockIterator.Reset();
                    }
                    Array.Copy(block.Ticks, r, block.Ticks, l, n-r);
                    block.used = l + n - r;
                    block.Modify();
                }
            }
            return nRemoved;        
        }

        private void addNewBlock(TimeSeriesTick t)
        {
            TimeSeriesBlock block = (TimeSeriesBlock)blockConstructor.Invoke(ClassDescriptor.noArgs);
            if (block == null) 
            {
                throw new StorageError(StorageError.ErrorCode.CONSTRUCTOR_FAILURE);
            }
            block.timestamp = t.Time;
            block.used = 1;
            block[0] = t;
            index.Put(block.timestamp, block);
        }

        private void insertInBlock(TimeSeriesBlock block, TimeSeriesTick tick)
        {
            long t = tick.Time;
            int i, n = block.used;
        
            int l = 0, r = n;
            while (l < r)  
            {
                i = (l+r) >> 1;
                if (t > block[i].Time) 
                { 
                    l = i+1;
                } 
                else 
                { 
                    r = i;
                }
            }
            Assert.That(l == r && (l == n || block[l].Time >= t));
            if (r == 0) 
            { 
                if (block[n-1].Time - t > maxBlockTimeInterval || n == block.Ticks.Length) 
                { 
                    addNewBlock(tick);
                    return;
                }
                block.timestamp = t;
            } 
            else if (r == n) 
            {
                if (t - block[0].Time > maxBlockTimeInterval || n == block.Ticks.Length) 
                { 
                    addNewBlock(tick);
                    return;
                } 
            }
            if (n == block.Ticks.Length) 
            { 
                addNewBlock(block[n-1]);
                Array.Copy(block.Ticks, r, block.Ticks, r+1, n-r-1);
            } 
            else 
            { 
                if (n != r) 
                { 
                    Array.Copy(block.Ticks, r, block.Ticks, r+1, n-r);
                }
                block.used += 1;
            }
            block[r] = tick;
            block.Modify();
        }

        private void lookupConstructor(Type cls)
        {
            blockConstructor = cls.GetConstructor(BindingFlags.Instance|BindingFlags.Instance|BindingFlags.Public|BindingFlags.NonPublic|BindingFlags.DeclaredOnly, null, ClassDescriptor.defaultConstructorProfile, null);
            if (blockConstructor == null) 
            { 
                throw new StorageError(StorageError.ErrorCode.DESCRIPTOR_FAILURE, cls);
            }
        }

        internal TimeSeriesImpl(Storage storage, Type blockClass, long maxBlockTimeInterval) 
        {
            lookupConstructor(blockClass);
            this.maxBlockTimeInterval = maxBlockTimeInterval;
            blockClassName = blockClass.FullName;
            index = storage.CreateIndex(typeof(long), true);
        }

        internal TimeSeriesImpl() {}
   
        public override void OnLoad() 
        {  
            lookupConstructor(ClassDescriptor.lookup(Storage, blockClassName));
        }

        private Index  index;
        private long   maxBlockTimeInterval;
        private string blockClassName;
        [NonSerialized()]
        private ConstructorInfo blockConstructor;
    }
}
 