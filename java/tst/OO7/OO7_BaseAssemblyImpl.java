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

public class OO7_BaseAssemblyImpl extends OO7_AssemblyImpl implements OO7_BaseAssembly {
    Link theComponentsPriv;
    Link theComponentsShar;
    
    
    protected OO7_BaseAssemblyImpl() {}

    public OO7_BaseAssemblyImpl(Storage storage) {
        theComponentsPriv = storage.createLink();
        theComponentsShar = storage.createLink();
    }
    
    
    public void addComponentsPriv( OO7_CompositePart x ) {
        theComponentsPriv.add( x );
        modify();
    } 
    
    
    public Link componetsPriv() {
        return theComponentsPriv;
    } 
    
    
    public void addComponentsShar( OO7_CompositePart x ) {
        theComponentsShar.add( x );
        modify();
    } 
    
    
    public Link componentsShar() {
        return theComponentsShar;
    } 
}
