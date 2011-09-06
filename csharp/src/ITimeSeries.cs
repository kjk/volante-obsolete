namespace Volante
{
    using System;
    using System.Collections.Generic;

    /// <summary>Interface for time series elements.
    /// Objects inserted into time series must implement this interface.
    /// </summary>
    public interface ITimeSeriesTick
    {
        /// <summary>
        /// Get time series element timestamp. Has the same meaning as DateTime.Ticks (100 nanoseconds). 
        /// </summary>
        long Ticks { get; }
    }

    /// <summary>Time series is used for efficiently handling of time series data. 
    /// Time series usually contains a very large number
    /// of small elements which are usually accessed in sucessive order. 
    /// To avoid overhead of loading elements from the disk one at a time,
    /// Volante groups several elements together and stores them 
    /// as single object (block).
    /// </summary>
    public interface ITimeSeries<T> : IPersistent, IResource, ICollection<T> where T : ITimeSeriesTick
    {
        /// <summary>
        /// Get forward iterator for time series elements in the given time interval
        /// </summary>
        /// <param name="from">inclusive time of the beginning of interval</param>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <returns>forward iterator within specified range</returns>
        IEnumerator<T> GetEnumerator(DateTime from, DateTime till);

        /// <summary>
        /// Get iterator for all time series elements
        /// </summary>
        /// <param name="order">direction of iteration</param>
        /// <returns>iterator in specified direction</returns>
        IEnumerator<T> GetEnumerator(IterationOrder order);

        /// <summary>
        /// Get forward iterator for time series elements in a given time interval
        /// </summary>
        /// <param name="from">inclusive time of the beginning  of interval</param>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <param name="order">direction of iteration</param>
        /// <returns>iterator within specified range in the specified direction</returns>
        IEnumerator<T> GetEnumerator(DateTime from, DateTime till, IterationOrder order);

        /// <summary>
        /// Get forward iterator for time series elements in a given time interval
        /// </summary>
        /// <param name="from">inclusive time of the beginning  of interval</param>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <returns>forward iterator within specified range</returns>
        IEnumerable<T> Range(DateTime from, DateTime till);

        /// <summary>
        /// Get iterator through all time series elements
        /// </summary>
        /// <param name="order">direction of iteration</param>
        /// <returns>iterator in specified direction</returns>
        IEnumerable<T> Range(IterationOrder order);

        /// <summary>
        /// Get iterator for time series elements belonging to the specified range
        /// </summary>
        /// <param name="from">inclusive time of the beginning  of interval</param>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <param name="order">direction of iteration</param>
        /// <returns>iterator within specified range in specified direction</returns>
        IEnumerable<T> Range(DateTime from, DateTime till, IterationOrder order);

        /// <summary>
        /// Get forward iterator for time series elements with timestamp greater or equal than specified
        /// </summary>
        /// <param name="from">inclusive time of the beginning of interval</param>
        /// <returns>forward iterator</returns>
        IEnumerable<T> From(DateTime from);

        /// <summary>
        /// Get backward iterator for time series elements with timestamp less or equal than specified
        /// </summary>
        /// <param name="till">inclusive time of the eding of interval</param>
        /// <returns>backward iterator</returns>
        IEnumerable<T> Till(DateTime till);

        /// <summary>
        /// Get backward iterator for time series elements 
        /// </summary>
        /// <returns>backward iterator</returns>
        IEnumerable<T> Reverse();

        /// <summary>
        /// Get timestamp of first element in time series
        /// </summary>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.ErrorClass.KEY_NOT_FOUND) if time series is empy</exception>
        DateTime FirstTime { get; }

        /// <summary>
        /// Get timestamp of last element in time series
        /// </summary>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.ErrorClass.KEY_NOT_FOUND) if time series is empy</exception>
        DateTime LastTime { get; }

        /// <summary> 
        /// Get element for a given timestamp
        /// </summary>
        /// <param name="timestamp">time series element timestamp</param>
        /// <exception cref="Volante.DatabaseException">DatabaseException(DatabaseException.ErrorClass.KEY_NOT_FOUND) if no element with such timestamp exists</exception>
        T this[DateTime timestamp]
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
        /// <param name="from">inclusive time of the beginning  of interval</param>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <returns>number of removed elements</returns>
        int Remove(DateTime from, DateTime till);

        /// <summary>
        /// Remove time series elements with timestamp greater or equal then specified
        /// </summary>
        /// <param name="from">inclusive time of the beginning of interval</param>
        /// <returns>number of removed elements</returns>
        int RemoveFrom(DateTime from);

        /// <summary>
        /// Remove elements with timestamp less or equal then specified
        /// </summary>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <returns>number of removed elements</returns>
        int RemoveTill(DateTime till);

        /// <summary>
        /// Remove all elements
        /// </summary>
        /// <returns>number of removed elements</returns>
        int RemoveAll();
    }
}
