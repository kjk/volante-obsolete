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


public interface OO7_ComplexAssembly extends OO7_Assembly {
    
    
    public void addSubAssembly( OO7_Assembly x );
    
    
    public Link subAssemblies();
}
