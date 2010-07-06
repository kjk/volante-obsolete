package org.garret.perst;

import java.util.Set;

/**
 * Interface of persistent set. 
 */
public interface IPersistentSet<T extends IPersistent> extends IPersistent, IResource, Set<T> {}
