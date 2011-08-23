namespace Volante
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Interface for timeseries element.
    /// You should derive your time series element from this class
    /// and implement Time getter method.
    /// </summary>
    public interface ITimeSeriesTick
    {
        /// <summary>
        /// Get time series element timestamp (100 nanoseconds)
        /// </summary>
        long Time { get; }
    }

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
    /// Volante need to know format of block class, it is responsibity of prgorammer
    /// to create block implementation derived from TimeSeriesBlock class
    /// and containing array of time series elements. Size of this array specifies
    /// the size of the block.
    /// </p>
    /// </summary>
    public interface ITimeSeries<T> : IPersistent, IResource, ICollection<T> where T : ITimeSeriesTick
    {

        /// <summary>
        /// Get forward iterator for time series elements belonging to the specified range
        /// </summary>
        /// <param name="from">inclusive time of the begging of interval</param>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <returns>forward iterator within specified range</returns>
        IEnumerator<T> GetEnumerator(DateTime from, DateTime till);

        /// <summary>
        /// Get iterator through all time series elements
        /// </summary>
        /// <param name="order">direction of iteration</param>
        /// <returns>iterator in specified direction</returns>
        IEnumerator<T> GetEnumerator(IterationOrder order);

        /// <summary>
        /// Get forward iterator for time series elements belonging to the specified range
        /// </summary>
        /// <param name="from">inclusive time of the begging of interval</param>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <param name="order">direction of iteration</param>
        /// <returns>iterator within specified range in specified direction</returns>
        IEnumerator<T> GetEnumerator(DateTime from, DateTime till, IterationOrder order);

        /// <summary>
        /// Get forward iterator for time series elements belonging to the specified range
        /// </summary>
        /// <param name="from">inclusive time of the begging of interval</param>
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
        /// <param name="from">inclusive time of the begging of interval</param>
        /// <param name="till">inclusive time of the ending of interval</param>
        /// <param name="order">direction of iteration</param>
        /// <returns>iterator within specified range in specified direction</returns>
        IEnumerable<T> Range(DateTime from, DateTime till, IterationOrder order);

        /// <summary>
        /// Get forward iterator for time series elements with timestamp greater or equal than specified
        /// </summary>
        /// <param name="from">inclusive time of the begging of interval</param>
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
        /// Get timestamp of first time series element
        /// </summary>
        /// <exception cref="Volante.DatabaseError">DatabaseError(DatabaseError.ErrorClass.KEY_NOT_FOUND) if time series is empy</exception>
        DateTime FirstTime { get; }

        /// <summary>
        /// Get timestamp of last time series element
        /// </summary>
        /// <exception cref="Volante.DatabaseError">DatabaseError(DatabaseError.ErrorClass.KEY_NOT_FOUND) if time series is empy</exception>
        DateTime LastTime { get; }

        /// <summary> 
        /// Get tick for specified data
        /// </summary>
        /// <param name="timestamp">time series element timestamp</param>
        /// <exception cref="Volante.DatabaseError">DatabaseError(DatabaseError.ErrorClass.KEY_NOT_FOUND) if no element with such timestamp exists</exception>
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
