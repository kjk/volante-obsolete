/*
 * Created on Jan 24, 2004
 */
package org.nachodb.aspectj;

/**
 * Base interface for all classes automatically treated as persistent capable.
 * Programmer should either explicitly add this interface to all classes which he want to be persistent
 * capable or add this interface using AspectJ <code>declare parents:</code> construction.
 * This interface doesn't allow to access fields of external (non-this) object,
 * you should use getter/setter methods instead.
 * If you want to provide access to external fields you should use StrictAutoPersist interface.

 * @author Patrick Morris-Suzuki
 *
 */
public interface AutoPersist {

}
