package org.nachodb;

/**
 * Interface of objects stored as value. Value objects are stored inside the persistent object to which they are belong
 * and not as separate instance. Value field can not conayins NULL value. When value object is changed, 
 * programmer should call <code>store</code> method of persistent calss containing this value. 
 */
public interface IValue { 
}
