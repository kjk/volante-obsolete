namespace NachoDB
{
    using System;
#if USE_GENERICS
    using System.Collections.Generic;
#else
    using System.Collections;
#endif

    /// <summary>
    /// Interface for timeseries element.
    /// You should derive your time series element from this class
    /// and implement Time getter method.
    /// </summary>
    public interface TimeSeriesTick 
    { 
        /// <summary>
        /// Get time series element timestamp (100 nanoseconds)
        /// </summary>
        long Time {get;}
    }

#if !USE_GENERICS
    /// <summary>
    /// Abstract base class for time series block.
    /// Progammer has to define its own block class derived from this class
    /// containign array of time series elements and providing accessors to the array elements 
    /// and Ticks getter method to access this whole array.
    /// </summary>
    public abstract class TimeSeriesBlock : Persistent 
    { 
        public long timestamp;
        public int  used;

        /// <summary>
        /// Get time series elements stored in this block.
        /// Returns preallocated array of time series element. Only <code>used</code>
        /// items of this array actually contains time series elements.
        /// </summary>
        public abstract Array Ticks{get;}

        /// <summary>
        /// Array elements accessor. 
        /// </summary>
        public abstract TimeSeriesTick this[int i] {get; set;}
    }
#endif

    /// <summary>
    /// <p>
    /// Time series interface. Time series class is used for efficient
    /// handling of time series data. Ussually time series contains a very large number
    /// if relatively small elements which are ussually acessed in sucessive order. 
    /// To avoid overhead of loading from the disk each particular time series element, 
    /// this class group several subsequent time series elements together and store them 
    /// as single object (block).
    /// </p><p> 
    /// As far as C# currently has no templates and
    /// Perst need to know format of block class, it is responsibity of prgorammer
    /// to create block implementation derived from TimeSeriesBlock class
    /// and containing array of time series elements. Size of this array specifies
    /// the size of the block.
    /// </p>
    /// </summary>
#if USE_GENERICS
    public interface TimeSeries<T> : IPersistent, IResource, ICollection<T> where T:TimeSeriesTick
#else
    public interface TimeSeries : IPersistent, IResource, ICollection 
#endif
    {    
#if !USE_GENERICS
        /// <summary>
        /// Add new tick to time series
        /// </summary>
        /// <param name="tick">new time series element</param>
        void Add(TimeSeriesTick tick);    
#endif

        /// <summary>
        /// Get forward iterator for time series elements belonging to the specified range
        /// </summary>
        /// <param name="from">inclusive time of the begging of interval</param>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <returns>forward iterator within specified range</returns>
#if USE_GENERICS
        IEnumerator<T> GetEnumerator(DateTime from, DateTime till);
#else
        IEnumerator GetEnumerator(DateTime from, DateTime till);
#endif

        /// <summary>
        /// Get iterator through all time series elements
        /// </summary>
        /// <param name="order">direction of iteration</param>
        /// <returns>iterator in specified direction</returns>
#if USE_GENERICS
        IEnumerator<T> GetEnumerator(IterationOrder order);
#else
        IEnumerator GetEnumerator(IterationOrder order);
#endif

        /// <summary>
        /// Get forward iterator for time series elements belonging to the specified range
        /// </summary>
        /// <param name="from">inclusive time of the begging of interval</param>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <param name="order">direction of iteration</param>
        /// <returns>iterator within specified range in specified direction</returns>
#if USE_GENERICS
        IEnumerator<T> GetEnumerator(DateTime from, DateTime till, IterationOrder order);
#else
        IEnumerator GetEnumerator(DateTime from, DateTime till, IterationOrder order);
#endif

        /// <summary>
        /// Get forward iterator for time series elements belonging to the specified range
        /// </summary>
        /// <param name="from">inclusive time of the begging of interval</param>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <returns>forward iterator within specified range</returns>
#if USE_GENERICS
        IEnumerable<T> Range(DateTime from, DateTime till);
#else
        IEnumerable Range(DateTime from, DateTime till);
#endif

        /// <summary>
        /// Get iterator through all time series elements
        /// </summary>
        /// <param name="order">direction of iteration</param>
        /// <returns>iterator in specified direction</returns>
#if USE_GENERICS
        IEnumerable<T> Range(IterationOrder order);
#else
        IEnumerable Range(IterationOrder order);
#endif

        /// <summary>
        /// Get forward iterator for time series elements belonging to the specified range
        /// </summary>
        /// <param name="from">inclusive time of the begging of interval</param>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <param name="order">direction of iteration</param>
        /// <returns>iterator within specified range in specified direction</returns>
#if USE_GENERICS
        IEnumerable<T> Range(DateTime from, DateTime till, IterationOrder order);
#else
        IEnumerable Range(DateTime from, DateTime till, IterationOrder order);
#endif

        /// <summary>
        /// Get forward iterator for time series elements with timestamp greater or equal than specified
        /// </summary>
        /// <param name="from">inclusive time of the begging of interval</param>
        /// <returns>forward iterator</returns>
#if USE_GENERICS
        IEnumerable<T> From(DateTime from);
#else
        IEnumerable From(DateTime from);
#endif

        /// <summary>
        /// Get backward iterator for time series elements with timestamp less or equal than specified
        /// </summary>
        /// <param name="till">inclusive time of the eding of interval</param>
        /// <returns>backward iterator</returns>
#if USE_GENERICS
        IEnumerable<T> Till(DateTime till);
#else
        IEnumerable Till(DateTime till);
#endif

        /// <summary>
        /// Get backward iterator for time series elements 
        /// </summary>
        /// <returns>backward iterator</returns>
#if USE_GENERICS
        IEnumerable<T> Reverse();
#else
        IEnumerable Reverse();
#endif

        /// <summary>
        /// Get timestamp of first time series element
        /// </summary>
        /// <exception cref="NachoDB.StorageError">StorageError(StorageError.ErrorClass.KEY_NOT_FOUND) if time series is empy</exception>
        DateTime FirstTime {get;}

        /// <summary>
        /// Get timestamp of last time series element
        /// </summary>
        /// <exception cref="NachoDB.StorageError">StorageError(StorageError.ErrorClass.KEY_NOT_FOUND) if time series is empy</exception>
        DateTime LastTime {get;}

        /// <summary> 
        /// Get tick for specified data
        /// </summary>
        /// <param name="timestamp">time series element timestamp</param>
        /// <exception cref="NachoDB.StorageError">StorageError(StorageError.ErrorClass.KEY_NOT_FOUND) if no element with such timestamp exists</exception>
#if USE_GENERICS
        T this[DateTime timestamp] 
#else
        TimeSeriesTick this[DateTime timestamp] 
#endif
        {
            get;
        }
    
        /// <summary>
        /// Check if data is available in time series for the specified time
        /// </summary>
        /// <param name="timestamp">time series element timestamp</param>
        /// <returns><code>true</code> if there is element in time series with such timestamp, 
        /// <code>false</code> otherwise</returns>
        bool Contains(DateTime timestamp);

        /// <summary>
        /// Remove time series elements belonging to the specified range
        /// </summary>
        /// <param name="from">inclusive time of the begging of interval</param>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <returns>number of removed elements</returns>
        int Remove(DateTime from, DateTime till);

        /// <summary>
        /// Remove time series elements with timestamp greater or equal then specified
        /// </summary>
        /// <param name="from">inclusive time of the begging of interval</param>
        /// <returns>number of removed elements</returns>
        int RemoveFrom(DateTime from);

        /// <summary>
        /// Remove time series elements with timestamp less or equal then specified
        /// </summary>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <returns>number of removed elements</returns>
        int RemoveTill(DateTime till);

        /// <summary>
        /// Remove all time series elements
        /// </summary>
        /// <returns>number of removed elements</returns>
        int RemoveAll();
    }
}
