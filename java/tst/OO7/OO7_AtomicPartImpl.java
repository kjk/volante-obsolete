// You can redistribute this software and/or modify it under the terms of
// the Ozone Core License version 1 published by ozone-db.org.
//
// The original code and portions created by Thorsten Fiebig are
// Copyright (C) 2000-@year@ by Thorsten Fiebig. All rights reserved.
// Code portions created by SMB are
// Copyright (C) 1997-@year@ by SMB GmbH. All rights reserved.
//
// $Id$

import org.garret.perst.*;


public class OO7_AtomicPartImpl extends OO7_DesignObjectImpl implements OO7_AtomicPart {
    long theX;
    long theY;
    long theDocId;
    Link theToConnections;
    Link theFromConnections;
    OO7_CompositePart thePartOf;
    
    
    private OO7_AtomicPartImpl() {}

    public OO7_AtomicPartImpl(Storage storage) {
        theToConnections = storage.createLink();
        theFromConnections = storage.createLink();
    }
    
    
    public void setX( long x ) {
        theX = x;
        modify();
    } 
    
    
    public long x() {
        return theX;
    } 
    
    
    public void setY( long x ) {
        theY = x;
        modify();
    } 
    
    
    public long y() {
        return theY;
    } 
    
    
    public void setDocId( long x ) {
        theDocId = x;
        modify();
    } 
    
    
    public long docId() {
        return theDocId;
    } 
    
    
    public void addTo( OO7_Connection x ) {
        theToConnections.add( x );
        modify();
    } 
    
    
    public Link to() {
        return theToConnections;
    } 
    
    
    public void addFrom( OO7_Connection x ) {
        theFromConnections.add( x );
        modify();
    } 
    
    
    public Link from() {
        return theFromConnections;
    } 
    
    
    public void setPartOf( OO7_CompositePart x ) {
        thePartOf = x;
        modify();
    } 
    
    
    public OO7_CompositePart partOf() {
        return thePartOf;
    } 
}
