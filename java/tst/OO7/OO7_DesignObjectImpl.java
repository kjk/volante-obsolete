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

public class OO7_DesignObjectImpl extends Persistent implements OO7_DesignObject {
    long theId;
    String theType;
    long theBuildDate;
    
    
    protected OO7_DesignObjectImpl() {}

    public OO7_DesignObjectImpl(String type) {
        theType = type;
    }
    
    
    public void setId( long x ) {
        theId = x;
        modify();
    } 
    
    
    public long id() {
        return theId;
    } 
    
    
    public void setType( String x ) {
        theType = x;
        modify();
    } 
    
    
    public String type() {
        return theType;
    } 
    
    
    public void setBuildDate( long x ) {
        theBuildDate = x;
        modify();
    } 
    
    
    public long buildDate() {
        return theBuildDate;
    } 
}
