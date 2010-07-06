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

public class OO7_DocumentImpl extends Persistent implements OO7_Document {
    String theTitle;
    long theId;
    String theText;
    
    
    public OO7_DocumentImpl() {
    }
    
    
    public void setTitle( String x ) {
        theTitle = x;
        modify();
    } 
    
    
    public String title() {
        return theTitle;
    } 
    
    
    public void setId( long x ) {
        theId = x;
        modify();
    } 
    
    
    public long id() {
        return theId;
    } 
    
    
    public void setText( String x ) {
        theText = x;
        modify();
    } 
    
    
    public String text() {
        return theText;
    } 
}
