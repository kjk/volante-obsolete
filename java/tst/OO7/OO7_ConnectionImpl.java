// You can redistribute this software and/or modify it under the terms of
// the Ozone Core License version 1 published by ozone-db.org.
//
// The original code and portions created by Thorsten Fiebig are
// Copyright (C) 2000-@year@ by Thorsten Fiebig. All rights reserved.
// Code portions created by SMB are
// Copyright (C) 1997-@year@ by SMB GmbH. All rights reserved.
//
// $Id$

import org.garret.perst.Persistent;


public class OO7_ConnectionImpl extends Persistent implements OO7_Connection {
    String theType;
    long theLength;
    OO7_AtomicPart theFrom;
    OO7_AtomicPart theTo;
    
    
    private OO7_ConnectionImpl() {}

    public OO7_ConnectionImpl(String type) {
        theType = type;
    }
    
    
    public void setType( String x ) {
        theType = x;
        modify();
    } 
    
    
    public String type() {
        return theType;
    } 
    
    
    public void setLength( long x ) {
        theLength = x;
        modify();
    } 
    
    
    public long length() {
        return theLength;
    } 
    
    
    public void setFrom( OO7_AtomicPart x ) {
        theFrom = x;
        modify();
    } 
    
    
    public OO7_AtomicPart from() {
        return theFrom;
    } 
    
    
    public void setTo( OO7_AtomicPart x ) {
        theTo = x;
        modify();
    } 
    
    
    public OO7_AtomicPart to() {
        return theTo;
    } 
}
