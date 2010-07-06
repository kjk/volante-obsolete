// You can redistribute this software and/or modify it under the terms of
// the Ozone Core License version 1 published by ozone-db.org.
//
// The original code and portions created by Thorsten Fiebig are
// Copyright (C) 2000-@year@ by Thorsten Fiebig. All rights reserved.
// Code portions created by SMB are
// Copyright (C) 1997-@year@ by SMB GmbH. All rights reserved.
//
// $Id$


public class OO7_AssemblyImpl extends OO7_DesignObjectImpl implements OO7_Assembly {
    OO7_ComplexAssembly theSuperAssembly;
    OO7_Module theModule;
    
    
    public void setSuperAssembly( OO7_ComplexAssembly x ) {
        theSuperAssembly = x;
    } 
    
    
    public OO7_ComplexAssembly superAssembly() {
        return theSuperAssembly;
    } 
    
    
    public void setModule( OO7_Module x ) {
        theModule = x;
        modify();
    } 
    
    
    public OO7_Module module() {
        return theModule;
    } 
}
