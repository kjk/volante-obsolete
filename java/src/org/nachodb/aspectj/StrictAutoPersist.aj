/*
 * Created on Feb 08, 2004
 */
package org.nachodb.aspectj;

/**
 * Base interface for all classes automatically treated as persistent capable which needs
 * to provide external access to their fields. Using this interface will significantly decrease 
 * efficiency of result code, because any access to instance fields of this class (doesn't matter access 
 * to self or foreign field) will be prepended by invocation of load() method. As far as 
 * OOP design rules recommend to made all fields private or protected and access them only through methods, 
 * I highly recommend you to avoid access to foreign fields and do not use this code. 
 * Access fields through getter methods will be in any case much efficient.
 */
public interface StrictAutoPersist extends AutoPersist {

}
