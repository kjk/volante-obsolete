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


public class OO7_CompositePartImpl extends OO7_DesignObjectImpl implements OO7_CompositePart {
    OO7_Document theDocumentation;
    Link theUsedInPriv;
    Link theUsedInShar;
    Link theParts;
    OO7_AtomicPart theRootPart;
    
    
    private OO7_CompositePartImpl() {}

    public OO7_CompositePartImpl(Storage storage) {
        theUsedInPriv = storage.createLink();
        theUsedInShar = storage.createLink();
        theParts = storage.createLink();
    }
    
    
    public void setDocumentation( OO7_Document x ) {
        theDocumentation = x;
        modify();
    } 
    
    
    public OO7_Document documentation() {
        return theDocumentation;
    } 
    
    
    public void addUsedInPriv( OO7_BaseAssembly x ) {
        theUsedInPriv.add( x );
        modify();
    } 
    
    
    public Link usedInPriv() {
        return theUsedInPriv;
    } 
    
    
    public void addUsedInShar( OO7_BaseAssembly x ) {
        theUsedInShar.add( x );
        modify();
    } 
    
    
    public Link usedInShar() {
        return theUsedInShar;
    } 
    
    
    public void addPart( OO7_AtomicPart x ) {
        theParts.add( x );
        modify();
    } 
    
    
    public Link parts() {
        return theParts;
    } 
    
    
    public void setRootPart( OO7_AtomicPart x ) {
        theRootPart = x;
        modify();
    } 
    
    
    public OO7_AtomicPart rootPart() {
        return theRootPart;
    } 
}
