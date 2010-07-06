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


public interface OO7_CompositePart extends OO7_DesignObject {
    
    
    public void setDocumentation( OO7_Document x );
    
    
    public OO7_Document documentation();
    
    
    public void addUsedInPriv( OO7_BaseAssembly x );
    
    
    public Link usedInPriv();
    
    
    public void addUsedInShar( OO7_BaseAssembly x );
    
    
    public Link usedInShar();
    
    
    public void addPart( OO7_AtomicPart x );
    
    
    public Link parts();
    
    
    public void setRootPart( OO7_AtomicPart x );
    
    
    public OO7_AtomicPart rootPart();
}
