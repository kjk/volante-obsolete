namespace Perst.Impl
{
    using System;
#if USE_GENERICS
    using System.Collections.Generic;
#else
    using System.Collections;
#endif
    using System.Reflection;
    using System.Diagnostics;
    using Perst;
	
#if USE_GENERICS
    class TimeSeriesImpl<T> : PersistentResource, TimeSeries<T> where T:TimeSeriesTick
#else
    class TimeSeriesImpl : PersistentCollection, TimeSeries
#endif

    { 
#if USE_GENERICS
        public virtual bool IsSynchronized 
        {
            get 
            {
                return true;
            }
        }

        public virtual object SyncRoot 
        {
            get 
            {
                return this;
            }
        }

        public void Clear() 
        {
            foreach (TimeSeriesBlock block in index) 
            {
                block.Deallocate();
            }
            index.Clear();
        }                

        public virtual void CopyTo(T[] dst, int i) 
        {
            foreach (object o in this) 
            { 
                dst.SetValue(o, i++);
            }
        }

        public virtual bool IsReadOnly 
        { 
            get
            { 
                return false;
            } 
        } 

        public bool Contains(T obj) 
        { 
            return Contains(new DateTime(obj.Time));
        }

        public bool Remove(T obj) 
        { 
            DateTime t = new DateTime(obj.Time);
            return Remove(t, t) != 0;
        }

        public class TimeSeriesBlock : Persistent 
        { 
            public long timestamp;
            public int  used;

            public T[]  Ticks;

            public T this[int i] 
            {
                get
                { 
                     return Ticks[i];
                }

                set
                {
                     Ticks[i] = value;
                }
            }

            public TimeSeriesBlock(int size) { 
                Ticks = new T[size];
            }

            TimeSeriesBlock() {}
        }
#endif


#if USE_GENERICS
        public void Add(T tick) 
#else
        public void Add(TimeSeriesTick tick) 
#endif
        { 
            long time = tick.Time;
            foreach (TimeSeriesBlock block in index.Range(time - maxBlockTimeInterval, time, IterationOrder.DescentOrder)) {
                insertInBlock(block, tick);
                return;
            } 
            addNewBlock(tick);
        }

#if USE_GENERICS
        class TimeSeriesEnumerator : IEnumerator<T>, IEnumerable<T>
#else
        class TimeSeriesEnumerator : IEnumerator, IEnumerable 
#endif
        { 
#if USE_GENERICS
            internal TimeSeriesEnumerator(IEnumerator<TimeSeriesBlock> blockIterator, long from, long till) 
#else
            internal TimeSeriesEnumerator(IEnumerator blockIterator, long from, long till) 
#endif
            { 
                this.till = till;
                this.from = from;
                this.blockIterator = blockIterator;
                Reset();
            }

#if USE_GENERICS
            public IEnumerator<T> GetEnumerator() 
#else
            public IEnumerator GetEnumerator() 
#endif
            { 
                return this;
            }
        
            public void Reset()
            {
#if !USE_GENERICS
                if (resetNeeded) 
                {
                    blockIterator.Reset();
                }
                resetNeeded = true;
#endif
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
                    Debug.Assert(l == r && (l == n || block[l].Time >= from)); 
                    if (l < n) 
                    {
                        pos = l;
                        currBlock = block;
                        return;
                    }
                } 
            }

            public void Dispose() {}

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

#if USE_GENERICS
            public virtual T Current 
#else
            public virtual object Current 
#endif
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

#if USE_GENERICS
            private IEnumerator<TimeSeriesBlock> blockIterator;
#else
            private IEnumerator     blockIterator;
            private bool            resetNeeded;
#endif
            private bool            hasCurrent;
            private TimeSeriesBlock currBlock;
            private int             pos;
            private long            from;
            private long            till;
        }
                
            
#if USE_GENERICS
        class TimeSeriesReverseEnumerator : IEnumerator<T>, IEnumerable<T>
#else
        class TimeSeriesReverseEnumerator : IEnumerator, IEnumerable
#endif
        { 
#if USE_GENERICS
            internal TimeSeriesReverseEnumerator(IEnumerator<TimeSeriesBlock> blockIterator, long from, long till) 
#else
            internal TimeSeriesReverseEnumerator(IEnumerator blockIterator, long from, long till) 
#endif
            { 
                this.till = till;
                this.from = from;
                this.blockIterator = blockIterator;
                Reset();
            }

#if USE_GENERICS
            public IEnumerator<T> GetEnumerator() 
#else
            public IEnumerator GetEnumerator() 
#endif
            { 
                return this;
            }

            public void Reset()
            {
#if !USE_GENERICS
                if (resetNeeded) 
                {
                    blockIterator.Reset();
                }
                resetNeeded = true;
#endif
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
                    Debug.Assert(l == r && (l == n || block[l].Time > till)); 
                    if (l > 0) 
                    {
                        pos = l-1;
                        currBlock = block;
                        return;
                    }
                } 
            }

            public void Dispose() {}

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

#if USE_GENERICS
            public virtual T Current 
#else
            public virtual object Current 
#endif
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

#if USE_GENERICS
            private IEnumerator<TimeSeriesBlock> blockIterator;
#else
            private IEnumerator     blockIterator;
            private bool            resetNeeded;
#endif
            private bool            hasCurrent;
            private TimeSeriesBlock currBlock;
            private int             pos;
            private long            from;
            private long            till;
        }
                
                            
#if USE_GENERICS
        public IEnumerator<T> GetEnumerator() 
#else
        public override IEnumerator GetEnumerator() 
#endif
        { 
            return iterator(0, Int64.MaxValue, IterationOrder.AscentOrder).GetEnumerator();
        }

#if USE_GENERICS
        public IEnumerator<T> GetEnumerator(DateTime from, DateTime till) 
#else
        public IEnumerator GetEnumerator(DateTime from, DateTime till) 
#endif
        {
            return iterator(from.Ticks, till.Ticks, IterationOrder.AscentOrder).GetEnumerator();
        }

#if USE_GENERICS
        public IEnumerator<T> GetEnumerator(DateTime from, DateTime till, IterationOrder order) 
#else
        public IEnumerator GetEnumerator(DateTime from, DateTime till, IterationOrder order) 
#endif
        {
            return iterator(from.Ticks, till.Ticks, order).GetEnumerator();
        }

#if USE_GENERICS
        public IEnumerator<T> GetEnumerator(IterationOrder order) 
#else
        public IEnumerator GetEnumerator(IterationOrder order) 
#endif
        {
            return iterator(0, Int64.MaxValue, order).GetEnumerator();
        }

#if USE_GENERICS
        public IEnumerable<T> Range(DateTime from, DateTime till) 
#else
        public IEnumerable Range(DateTime from, DateTime till) 
#endif
        {
            return iterator(from.Ticks, till.Ticks, IterationOrder.AscentOrder);
        }

#if USE_GENERICS
        public IEnumerable<T> Range(DateTime from, DateTime till, IterationOrder order) 
#else
        public IEnumerable Range(DateTime from, DateTime till, IterationOrder order) 
#endif
        {
            return iterator(from.Ticks, till.Ticks, order);
        }

#if USE_GENERICS
        public IEnumerable<T> Range(IterationOrder order) 
#else
        public IEnumerable Range(IterationOrder order) 
#endif
        {
            return iterator(0, Int64.MaxValue, order);
        }

#if USE_GENERICS
        public IEnumerable<T> Till(DateTime till) 
#else
        public IEnumerable Till(DateTime till) 
#endif
        { 
            return iterator(0, till.Ticks, IterationOrder.DescentOrder);
        }

#if USE_GENERICS
        public IEnumerable<T> From(DateTime from) 
#else
        public IEnumerable From(DateTime from) 
#endif
        { 
            return iterator(from.Ticks, Int64.MaxValue, IterationOrder.AscentOrder);
        }

#if USE_GENERICS
        public IEnumerable<T> Reverse() 
#else
        public IEnumerable Reverse() 
#endif
        { 
            return iterator(0, Int64.MaxValue, IterationOrder.DescentOrder);
        }


#if USE_GENERICS
        private IEnumerable<T> iterator(long from, long till, IterationOrder order) 
        { 
            IEnumerator<TimeSeriesBlock> enumerator = index.GetEnumerator(from - maxBlockTimeInterval, till, order);
            return order == IterationOrder.AscentOrder
                ? (IEnumerable<T>)new TimeSeriesEnumerator(enumerator, from, till)
                : (IEnumerable<T>)new TimeSeriesReverseEnumerator(enumerator, from, till);
        }
#else
        private IEnumerable iterator(long from, long till, IterationOrder order) 
        { 
            IEnumerator enumerator = index.GetEnumerator(from - maxBlockTimeInterval, till, order);
            return order == IterationOrder.AscentOrder
                ? (IEnumerable)new TimeSeriesEnumerator(enumerator, from, till)
                : (IEnumerable)new TimeSeriesReverseEnumerator(enumerator, from, till);
        }
#endif

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

#if USE_GENERICS
        public int Count 
#else
        public override int Count 
#endif
        {
            get 
            {
                int n = 0;
                foreach (TimeSeriesBlock block in index) 
                {
                    n += block.used;
                }
                return n;
            }
        }
       
#if USE_GENERICS
        public T this[DateTime timestamp]     
#else
        public TimeSeriesTick this[DateTime timestamp]     
#endif
        {
            get 
            {
                long time = timestamp.Ticks;
                foreach (TimeSeriesBlock block in index.Range(time - maxBlockTimeInterval, time, IterationOrder.AscentOrder))
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
                    Debug.Assert(l == r && (l == n || block[l].Time >= time)); 
                    if (l < n && block[l].Time == time) 
                    { 
                        return block[l];
                    }
                }
                throw new StorageError(StorageError.ErrorCode.KEY_NOT_FOUND);
            }
        }

        public bool Contains(DateTime timestamp) 
        {
            return this[timestamp] != null;
        }

        public int Remove(DateTime from, DateTime till) 
        {
            return remove(from.Ticks, till.Ticks);
        }

        public int RemoveFrom(DateTime from) 
        {
            return remove(from.Ticks, Int64.MaxValue);
        }

        public int RemoveTill(DateTime till) 
        {
            return remove(0, till.Ticks);
        }

        public int RemoveAll() 
        {
            return remove(0, Int64.MaxValue);
        }

        private int remove(long from, long till)
        {
            int nRemoved = 0;
#if USE_GENERICS
            IEnumerator<TimeSeriesBlock> blockIterator = index.GetEnumerator(from - maxBlockTimeInterval, till);
#else
            IEnumerator blockIterator = index.GetEnumerator(from - maxBlockTimeInterval, till);
#endif
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
                Debug.Assert(l == r && (l == n || block[l].Time >= from)); 
                while (r < n && block[r].Time <= till) 
                {
                    r += 1;
                    nRemoved += 1;
                }
                if (l == 0 && r == n) 
                { 
                    index.Remove(block.timestamp, block);
                    block.Deallocate();
#if USE_GENERICS
                    blockIterator = index.GetEnumerator(from - maxBlockTimeInterval, till);
#else
                    blockIterator.Reset();
#endif
                } 
                else if (l != r) 
                { 
                    if (l == 0) 
                    { 
                        index.Remove(block.timestamp, block);
                        block.timestamp = block[r].Time;
                        index.Put(block.timestamp, block);
#if USE_GENERICS
                        blockIterator = index.GetEnumerator(from - maxBlockTimeInterval, till);
#else
                        blockIterator.Reset();
#endif
                    }
                    Array.Copy(block.Ticks, r, block.Ticks, l, n-r);
                    block.used = l + n - r;
                    block.Modify();
                }
            }
            return nRemoved;        
        }

#if USE_GENERICS
        private void addNewBlock(T t)
        {
            TimeSeriesBlock block = new TimeSeriesBlock(blockSize);
#else
        private void addNewBlock(TimeSeriesTick t)
        {
            TimeSeriesBlock block = (TimeSeriesBlock)blockConstructor.Invoke(ClassDescriptor.noArgs);
            if (block == null) 
            {
                throw new StorageError(StorageError.ErrorCode.CONSTRUCTOR_FAILURE);
            }
#endif
            block.timestamp = t.Time;
            block.used = 1;
            block[0] = t;
            index.Put(block.timestamp, block);
        }

#if USE_GENERICS
        private void insertInBlock(TimeSeriesBlock block, T tick)
#else
        private void insertInBlock(TimeSeriesBlock block, TimeSeriesTick tick)
#endif
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
            Debug.Assert(l == r && (l == n || block[l].Time >= t));
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

#if USE_GENERICS
        internal TimeSeriesImpl(Storage storage, int blockSize, long maxBlockTimeInterval) 
        {
            this.blockSize = blockSize;
            this.maxBlockTimeInterval = maxBlockTimeInterval;
            index = storage.CreateIndex<long,TimeSeriesBlock>(true);
        }
#else
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
            blockClassName = ClassDescriptor.getTypeName(blockClass);
            index = storage.CreateIndex(typeof(long), true);
        }

        public override void OnLoad() 
        {  
            lookupConstructor(ClassDescriptor.lookup(Storage, blockClassName));
        }
#endif
        internal TimeSeriesImpl() {}
   
        public override void Deallocate() 
        {
            foreach (TimeSeriesBlock block in index) 
            {
                block.Deallocate();
            }
            index.Deallocate();
            base.Deallocate();
        }

#if USE_GENERICS
        private Index<long,TimeSeriesBlock> index;
        private long                        maxBlockTimeInterval;        
        private int                         blockSize;
#else
        private Index  index;
        private long   maxBlockTimeInterval;        
        private string blockClassName;
        [NonSerialized()]
        private ConstructorInfo blockConstructor;
#endif
    }
}
 