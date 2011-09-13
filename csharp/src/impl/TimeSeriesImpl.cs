namespace Volante.Impl
{
    using System;
    using System.Collections.Generic;
    using System.Collections;
    using System.Reflection;
    using System.Diagnostics;
    using Volante;

    class TimeSeriesImpl<T> : PersistentResource, ITimeSeries<T> where T : ITimeSeriesTick
    {
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
            return Contains(new DateTime(obj.Ticks));
        }

        public bool Remove(T obj)
        {
            DateTime t = new DateTime(obj.Ticks);
            return Remove(t, t) != 0;
        }

        public class TimeSeriesBlock : Persistent
        {
            public long timestamp;
            public int used;

            public T[] Ticks;

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

            public TimeSeriesBlock(int size)
            {
                Ticks = new T[size];
            }

            TimeSeriesBlock() { }
        }

        public void Add(T tick)
        {
            long time = tick.Ticks;
            foreach (TimeSeriesBlock block in index.Range(time - maxBlockTimeInterval, time, IterationOrder.DescentOrder))
            {
                insertInBlock(block, tick);
                return;
            }
            addNewBlock(tick);
        }

        class TimeSeriesEnumerator : IEnumerator<T>, IEnumerable<T>
        {
            internal TimeSeriesEnumerator(IEnumerator<TimeSeriesBlock> blockIterator, long from, long till)
            {
                this.till = till;
                this.from = from;
                this.blockIterator = blockIterator;
                Reset();
            }

            public IEnumerator<T> GetEnumerator()
            {
                return this;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public void Reset()
            {
                hasCurrent = false;
                blockIterator.Reset();
                pos = -1;
                while (blockIterator.MoveNext())
                {
                    TimeSeriesBlock block = (TimeSeriesBlock)blockIterator.Current;
                    int n = block.used;
                    int l = 0, r = n;
                    while (l < r)
                    {
                        int i = (l + r) >> 1;
                        if (from > block[i].Ticks)
                            l = i + 1;
                        else
                            r = i;
                    }
                    Debug.Assert(l == r && (l == n || block[l].Ticks >= from));
                    if (l < n)
                    {
                        pos = l;
                        currBlock = block;
                        return;
                    }
                }
            }

            public void Dispose() { }

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
                if (currBlock[pos].Ticks > till)
                {
                    pos = -1;
                    return false;
                }
                hasCurrent = true;
                return true;
            }

            public virtual T Current
            {
                get
                {
                    if (!hasCurrent)
                        throw new InvalidOperationException();
                    return currBlock[pos];
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return Current;
                }
            }

            private IEnumerator<TimeSeriesBlock> blockIterator;
            private bool hasCurrent;
            private TimeSeriesBlock currBlock;
            private int pos;
            private long from;
            private long till;
        }

        class TimeSeriesReverseEnumerator : IEnumerator<T>, IEnumerable<T>
        {
            internal TimeSeriesReverseEnumerator(IEnumerator<TimeSeriesBlock> blockIterator, long from, long till)
            {
                this.till = till;
                this.from = from;
                this.blockIterator = blockIterator;
                Reset();
            }

            public IEnumerator<T> GetEnumerator()
            {
                return this;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public void Reset()
            {
                hasCurrent = false;
                pos = -1;
                blockIterator.Reset();
                while (blockIterator.MoveNext())
                {
                    TimeSeriesBlock block = (TimeSeriesBlock)blockIterator.Current;
                    int n = block.used;
                    int l = 0, r = n;
                    while (l < r)
                    {
                        int i = (l + r) >> 1;
                        if (till >= block[i].Ticks)
                            l = i + 1;
                        else
                            r = i;
                    }
                    Debug.Assert(l == r && (l == n || block[l].Ticks > till));
                    if (l > 0)
                    {
                        pos = l - 1;
                        currBlock = block;
                        return;
                    }
                }
            }

            public void Dispose() { }

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
                            pos = currBlock.used - 1;
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
                if (currBlock[pos].Ticks < from)
                {
                    pos = -1;
                    return false;
                }
                hasCurrent = true;
                return true;
            }

            public virtual T Current
            {
                get
                {
                    if (!hasCurrent)
                        throw new InvalidOperationException();
                    return currBlock[pos];
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return Current;
                }
            }

            private IEnumerator<TimeSeriesBlock> blockIterator;
            private bool hasCurrent;
            private TimeSeriesBlock currBlock;
            private int pos;
            private long from;
            private long till;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return iterator(0, Int64.MaxValue, IterationOrder.AscentOrder).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IEnumerator<T> GetEnumerator(DateTime from, DateTime till)
        {
            return iterator(from.Ticks, till.Ticks, IterationOrder.AscentOrder).GetEnumerator();
        }

        public IEnumerator<T> GetEnumerator(DateTime from, DateTime till, IterationOrder order)
        {
            return iterator(from.Ticks, till.Ticks, order).GetEnumerator();
        }

        public IEnumerator<T> GetEnumerator(IterationOrder order)
        {
            return iterator(0, Int64.MaxValue, order).GetEnumerator();
        }

        public IEnumerable<T> Range(DateTime from, DateTime till)
        {
            return iterator(from.Ticks, till.Ticks, IterationOrder.AscentOrder);
        }

        public IEnumerable<T> Range(DateTime from, DateTime till, IterationOrder order)
        {
            return iterator(from.Ticks, till.Ticks, order);
        }

        public IEnumerable<T> Range(IterationOrder order)
        {
            return iterator(0, Int64.MaxValue, order);
        }

        public IEnumerable<T> Till(DateTime till)
        {
            return iterator(0, till.Ticks, IterationOrder.DescentOrder);
        }

        public IEnumerable<T> From(DateTime from)
        {
            return iterator(from.Ticks, Int64.MaxValue, IterationOrder.AscentOrder);
        }

        public IEnumerable<T> Reverse()
        {
            return iterator(0, Int64.MaxValue, IterationOrder.DescentOrder);
        }

        private IEnumerable<T> iterator(long from, long till, IterationOrder order)
        {
            IEnumerator<TimeSeriesBlock> enumerator = index.GetEnumerator(from - maxBlockTimeInterval, till, order);
            return order == IterationOrder.AscentOrder
                ? (IEnumerable<T>)new TimeSeriesEnumerator(enumerator, from, till)
                : (IEnumerable<T>)new TimeSeriesReverseEnumerator(enumerator, from, till);
        }

        public DateTime FirstTime
        {
            get
            {
                foreach (TimeSeriesBlock block in index)
                {
                    return new DateTime(block.timestamp);
                }
                throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND);
            }
        }

        public DateTime LastTime
        {
            get
            {
                foreach (TimeSeriesBlock block in index.Range(null, null, IterationOrder.DescentOrder))
                {
                    return new DateTime(block[block.used - 1].Ticks);
                }
                throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND);
            }
        }

        public int Count
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

        public T this[DateTime timestamp]
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
                        int i = (l + r) >> 1;
                        if (time > block[i].Ticks)
                            l = i + 1;
                        else
                            r = i;
                    }
                    Debug.Assert(l == r && (l == n || block[l].Ticks >= time));
                    if (l < n && block[l].Ticks == time)
                        return block[l];
                }
                throw new DatabaseException(DatabaseException.ErrorCode.KEY_NOT_FOUND);
            }
        }

        public bool Contains(DateTime timestamp)
        {
            try
            {
                T val = this[timestamp];
                return true;
            }
            catch (DatabaseException e)
            {
                if (e.Code == DatabaseException.ErrorCode.KEY_NOT_FOUND)
                    return false;
                throw;
            }
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
            IEnumerator<TimeSeriesBlock> blockIterator = index.GetEnumerator(from - maxBlockTimeInterval, till);

            while (blockIterator.MoveNext())
            {
                TimeSeriesBlock block = (TimeSeriesBlock)blockIterator.Current;
                int n = block.used;
                int l = 0, r = n;
                while (l < r)
                {
                    int i = (l + r) >> 1;
                    if (from > block[i].Ticks)
                        l = i + 1;
                    else
                        r = i;
                }
                Debug.Assert(l == r && (l == n || block[l].Ticks >= from));
                while (r < n && block[r].Ticks <= till)
                {
                    r += 1;
                    nRemoved += 1;
                }
                if (l == 0 && r == n)
                {
                    index.Remove(block.timestamp, block);
                    block.Deallocate();
                    blockIterator = index.GetEnumerator(from - maxBlockTimeInterval, till);
                }
                else if (l != r)
                {
                    if (l == 0)
                    {
                        index.Remove(block.timestamp, block);
                        block.timestamp = block[r].Ticks;
                        index.Put(block.timestamp, block);
                        blockIterator = index.GetEnumerator(from - maxBlockTimeInterval, till);
                    }
                    Array.Copy(block.Ticks, r, block.Ticks, l, n - r);
                    block.used = l + n - r;
                    block.Modify();
                }
            }
            return nRemoved;
        }

        private void addNewBlock(T t)
        {
            TimeSeriesBlock block = new TimeSeriesBlock(blockSize);
            block.timestamp = t.Ticks;
            block.used = 1;
            block[0] = t;
            index.Put(block.timestamp, block);
        }

        private void insertInBlock(TimeSeriesBlock block, T tick)
        {
            long t = tick.Ticks;
            int i, n = block.used;

            int l = 0, r = n;
            while (l < r)
            {
                i = (l + r) >> 1;
                if (t > block[i].Ticks)
                    l = i + 1;
                else
                    r = i;
            }
            Debug.Assert(l == r && (l == n || block[l].Ticks >= t));
            if (r == 0)
            {
                if (block[n - 1].Ticks - t > maxBlockTimeInterval || n == block.Ticks.Length)
                {
                    addNewBlock(tick);
                    return;
                }
                block.timestamp = t;
            }
            else if (r == n)
            {
                if (t - block[0].Ticks > maxBlockTimeInterval || n == block.Ticks.Length)
                {
                    addNewBlock(tick);
                    return;
                }
            }
            if (n == block.Ticks.Length)
            {
                addNewBlock(block[n - 1]);
                Array.Copy(block.Ticks, r, block.Ticks, r + 1, n - r - 1);
            }
            else
            {
                if (n != r)
                    Array.Copy(block.Ticks, r, block.Ticks, r + 1, n - r);
                block.used += 1;
            }
            block[r] = tick;
            block.Modify();
        }

        internal TimeSeriesImpl(IDatabase db, int blockSize, long maxBlockTimeInterval)
        {
            this.blockSize = blockSize;
            this.maxBlockTimeInterval = maxBlockTimeInterval;
            index = db.CreateIndex<long, TimeSeriesBlock>(IndexType.Unique);
        }
        internal TimeSeriesImpl() { }

        public override void Deallocate()
        {
            foreach (TimeSeriesBlock block in index)
            {
                block.Deallocate();
            }
            index.Deallocate();
            base.Deallocate();
        }

        private IIndex<long, TimeSeriesBlock> index;
        private long maxBlockTimeInterval;
        private int blockSize;
    }
}
