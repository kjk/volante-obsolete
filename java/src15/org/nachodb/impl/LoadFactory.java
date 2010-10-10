package org.nachodb.impl;

public interface LoadFactory { 
    Object create(ClassDescriptor desc);
}