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


public interface OO7_Module extends OO7_DesignObject {
    
    
    public void setManual( OO7_Manual x );
    
    
    public OO7_Manual manual();
    
    
    public void addAssembly( OO7_Assembly x );
    
    
    public Link assembly();
    
    
    public void setDesignRoot( OO7_ComplexAssembly x );
    
    
    public OO7_ComplexAssembly designRoot();


    public OO7_AtomicPart getAtomicPartByName(String name);


    public void addAtomicPart(String name, OO7_AtomicPart part);
}
