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

public class OO7_ModuleImpl extends OO7_DesignObjectImpl implements OO7_Module {
    OO7_Manual theManual;
    Link       theAssembly;
    Index      theComponents;
    OO7_ComplexAssembly theDesignRoot;
    
    
    private OO7_ModuleImpl() {}

    public OO7_ModuleImpl(Storage storage) {
        theAssembly = storage.createLink();
        theComponents = storage.createIndex(String.class, true);
    }
    
    
    public void setManual( OO7_Manual x ) {
        theManual = x;
        modify();
    } 
    
    
    public OO7_Manual manual() {
        return theManual;
    } 
    
    
    public void addAssembly( OO7_Assembly x ) {
        theAssembly.add( x );
        modify();
    } 
    
    
    public Link assembly() {
        return theAssembly;
    } 
    
    
    public void setDesignRoot( OO7_ComplexAssembly x ) {
        theDesignRoot = x;
        modify();
    } 
    
    
    public OO7_ComplexAssembly designRoot() {
        return theDesignRoot;
    } 

    public OO7_AtomicPart getAtomicPartByName(String name)
    {
        return (OO7_AtomicPart)theComponents.get(name);
    }


    public void addAtomicPart(String name, OO7_AtomicPart part)
    {
        theComponents.put(name, part);
    }
}
