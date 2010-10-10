package org.nachodb;

import java.util.*;

/**
 * Interface combining both Iterable and Iterator functionality
 */
public abstract class IterableIterator<T> implements Iterable<T>, Iterator<T> {
    /**
     * This class itself is iterator
     */
    public Iterator<T> iterator() { 
        return this;
    }
}
