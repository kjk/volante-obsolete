package org.nachodb;

import java.util.*;

/**
 * <p>
 * Time series interface. Time series class is used for efficient
 * handling of time series data. Ussually time series contains a very large number
 * if relatively small elements which are ussually acessed in sucessive order. 
 * To avoid overhead of loading from the disk each particular time series element, 
 * this class group several subsequent time series elements together and store them 
 * as single object (block).
 * </p><p> 
 * As far as Java currently has no templates and
 * Perst need to know format of block class, it is responsibity of prgorammer
 * to create block implementation derived from TimeSeries.Block class
 * and containing array of time series elements. Size of this array specifies
 * the size of the block.
 * </p>
 */
public interface TimeSeries<T extends TimeSeries.Tick> extends IPersistent, IResource, Collection<T> { 
    /**
     * Interface for timeseries element.
     * You should derive your time series element from this class
     * and implement getTime method.
     */
    public interface Tick extends IValue { 
        /**
         * Get time series element timestamp
         * @return timestamp in milliseconds
         */
        long getTime();
    }
    
    /**
     * Abstract base class for time series block.
     * Progammer has to define its own block class derived from this class
     * containign array of time series elements and providing getTicks()
     * method to access this array. It is better no to initialize this array in constructor
     * (because it will be also used when block will be loaded from the disk), 
     * but check in getTicks() method that array is null, and if so - create new array.
     */
    public static abstract class Block extends Persistent { 
        public long timestamp;
        public int  used;

        /**
         * Get time series elements stored in this block.
         * @return preallocated array of time series element. Only <code>used</code>
         * items of this array actually contains time series elements.
         * But all array items should be not null and conain referen to Tick object.
         */
        public abstract Tick[] getTicks();
    }

    /**
     * Add new tick to time series
     * @param tick new time series element
     */
    boolean add(T tick);    

    /**
     * Get list of alements in the time series (in ascending order)
     * @return list of all elements
     */
    ArrayList<T> elements();

    /**
     * Get forward iterator through all time series elements
     * @return forward iterator 
     */
    Iterator<T> iterator();

    /**
     * Get forward iterator for time series elements belonging to the specified range
     * @param from inclusive time of the begging of interval, 
     * if null then take all elements from the beginning of time series
     * @param till inclusive time of the ending of interval, 
     * if null then take all elements till the end of time series
     * @return forward iterator within specified range.
     */
    IterableIterator<T> iterator(Date from, Date till);

    /**
     * Get iterator through all time series elements
     * @param ascent direction of iteration
     * @return iterator in specified direction
     */
    IterableIterator<T> iterator(boolean ascent);

    /**
     * Get forward iterator for time series elements belonging to the specified range
     * @param from inclusive time of the begging of interval, 
     * if null then take all elements from the beginning of time series
     * @param till inclusive time of the ending of interval, 
     * if null then take all elements till the end of time series
     * @param ascent direction of iteration
     * @return iterator within specified range in specified direction
     */
    IterableIterator<T> iterator(Date from, Date till, boolean ascent);

    /**
     * Get timestamp of first time series element
     * @return time of time series start
     */
    Date getFirstTime();

    /**
     * Get timestamp of last time series element
     * @return time of time series end
     */
    Date getLastTime();

    /**
     * Get number of elements in time series
     * @return number of elements in time series
     */
    int size();

    /** 
     * Get tick for specified data
     * @param timestamp time series element timestamp
     * return time series element for the specified timestamp or null
     * if no such element was found
     */
    T getTick(Date timestamp);
    
    /**
     * Check if data is available in time series for the specified time
     * @param timestamp time series element timestamp
     * @return <code>true</code> if there is element in time series with such timestamp, 
     * <code>false</code> otherwise
     */
    boolean has(Date timestamp);

    /**
     * Remove timeseries elements belonging to the specified range
     * @param from inclusive time of the begging of interval, 
     * if null then remove all elements from the beginning of time series
     * @param till inclusive time of the ending of interval, 
     * if null then remove all elements till the end of time series
     * @return number of removed elements
     */
    int remove(Date from, Date till);
}

    