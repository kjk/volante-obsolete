// You can redistribute this software and/or modify it under the terms of
// the Ozone Core License version 1 published by ozone-db.org.
//
// The original code and portions created by Thorsten Fiebig are
// Copyright (C) 2000-@year@ by Thorsten Fiebig. All rights reserved.
// Code portions created by SMB are
// Copyright (C) 1997-@year@ by SMB GmbH. All rights reserved.
//
// $Id$

import org.garret.perst.Link;


public interface OO7_AtomicPart extends OO7_DesignObject {
    
    
    public void setX( long x );
    
    
    public long x();
    
    
    public void setY( long y );
    
    
    public long y();
    
    
    public void setDocId( long y );
    
    
    public long docId();
    
    
    public void addTo( OO7_Connection x );
    
    
    public Link to();
    
    
    public void addFrom( OO7_Connection x );
    
    
    public Link from();
    
    
    public void setPartOf( OO7_CompositePart x );
    
    
    public OO7_CompositePart partOf();
    
}
