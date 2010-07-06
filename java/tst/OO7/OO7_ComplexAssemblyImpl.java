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


public class OO7_ComplexAssemblyImpl extends OO7_AssemblyImpl implements OO7_ComplexAssembly {
    Link theSubAssemblies;
    
    
    private OO7_ComplexAssemblyImpl() {
    }

    public OO7_ComplexAssemblyImpl(Storage storage) {
        theSubAssemblies = storage.createLink();
    }
    
    
    public void addSubAssembly( OO7_Assembly x ) {
        theSubAssemblies.add( x );
        modify();
    } 
    
    
    public Link subAssemblies() {
        return theSubAssemblies;
    } 
}
