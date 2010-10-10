/*
 * Created on Jan 25, 2004
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package org.nachodb.aspectj;

/**
 * @author Patrick Morris-Suzuki
 *
 */

public aspect SafeHashAspect {

        declare precedence: PersistenceAspect+, SafeHashAspect;
        
        int around(SafeHashCode me):
                        execution(int SafeHashCode+.hashCode()) && target(me){
                return me.safeHashCode();
        }
}
 