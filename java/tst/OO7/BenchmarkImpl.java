// You can redistribute this software and/or modify it under the terms of
// the Ozone Core License version 1 published by ozone-db.org.
//
// The original code and portions created by Thorsten Fiebig are
// Copyright (C) 2000-@year@ by Thorsten Fiebig. All rights reserved.
// Code portions created by SMB are
// Copyright (C) 1997-@year@ by SMB GmbH. All rights reserved.
//
// $Id$

import java.util.*;

import org.garret.perst.*;


public class BenchmarkImpl extends Persistent implements Benchmark {
    // data base parameters
    private final static int fTest1Conn = 0;
    private final static int fTest3Conn = 1;
    private final static int fTiny = 2;
    private final static int fSmall = 3;
    
    private final static int[] fNumAtomicPerComp = {20, 20, 20, 20};
    private final static int[] fConnPerAtomic = {1, 3, 3, 3};
    private final static int[] fDocumentSize = {20, 20, 20, 2000};
    private final static int[] fManualSize = {1000, 1000, 1000, 100000};
    private final static int[] fNumCompPerModule = {5, 5, 50, 500};
    private final static int[] fNumAssmPerAssm = {3, 3, 3, 3};
    private final static int[] fNumAssmLevels = {3, 3, 7, 7};
    private final static int[] fNumCompPerAssm = {3, 3, 3, 3};
    private final static int[] fNumModules = {1, 1, 1, 1};
    
    final static boolean verbose = false;
    
    static Random theRandom = null;
    
    int theScale = 0;
    
    long theOid = 0;
    
    OO7_Module theModule = null;
    
    private final static int pagePoolSize = 32*1024*1024;
   
    public static void main( String[] args ) throws Exception {
        if (args.length == 0) {
            printUsage();
            System.exit( 1 );
        } else {
            if (args.length == 1 && args[1] == "query") {
                printUsage();
                System.exit( 1 );
            } 
        } 
        
        Storage db = StorageFactory.getInstance().createStorage();

        db.open("007.dbs", pagePoolSize);
            
        long start = System.currentTimeMillis();
        
        if (args[0].equals( "query" )) {
            if (args[1].equals( "traversal" )) {
                Benchmark anBenchmark = (Benchmark)db.getRoot();
                anBenchmark.traversalQuery();
            } else {
                if (args[1].equals( "match" )) {
                    Benchmark anBenchmark = (Benchmark)db.getRoot();
                    anBenchmark.matchQuery();
                } 
            } 
        } else {
            if (args[0].equals( "create" )) {
                int scale = -1;
                if (args[1].equals( "test3Conn" )) {
                    scale = fTest3Conn;
                } else if (args[1].equals( "test1Conn" )) {
                    scale = fTest1Conn;
                } else if (args[1].equals( "tiny" )) {
                    scale = fTiny;
                } else if (args[1].equals( "small" )) {
                    scale = fSmall;
                } else {
                    System.out.println( "Invalid scale" );
                    System.exit( 1 );
                } 
                Benchmark anBenchmark = new BenchmarkImpl();
                db.setRoot(anBenchmark);
                anBenchmark.create( scale );
            } 
        } 
            
        System.out.println( "time: " + (System.currentTimeMillis() - start) + "msec" );
        
        // close the connection
        db.close();
    } 
    
    
    static void printUsage() {
        System.out.println( "usage: OO7 (create|query) [options]" );
        System.out.println( "    create options:" );
        System.out.println( "        size        - (tiny|small|large)" );
        System.out.println( "    query options:" );
        System.out.println( "        type        - (traversal|match)" );
    } 
    
    
    static int getRandomInt( int lower, int upper ) {
        if (theRandom == null) {
            theRandom = new Random();
        } 
        
        int rVal;
        do {
            rVal = theRandom.nextInt();
            rVal %= upper;
        //System.out.println("rVal: " + rVal + " lower: " + lower + " upper: " + upper);
        } while (rVal < lower || rVal >= upper);
        return rVal;
    } 
    
    
    protected long getAtomicPartOid() {
        return theOid++;
    } 
    
    
    public void create( int anScale ) throws Exception {
        theScale = anScale;
        createModule();
    } 
    
    
    public void traversalQuery() throws Exception {
        Hashtable table = new Hashtable();
        long time = System.currentTimeMillis();
        traversal( theModule.designRoot(), table );
        time = (System.currentTimeMillis() - time);
        System.out.println( "Millis: " + time );
    } 
    
    
    protected void traversal( OO7_Assembly anAssembly, Hashtable aTable ) throws Exception {
        if (anAssembly instanceof OO7_BaseAssembly) {
            // System.out.println( "Base Assembly Class: " );
            OO7_BaseAssembly baseAssembly = (OO7_BaseAssembly)anAssembly;
            Iterator compIterator = baseAssembly.componentsShar().iterator();
            while (compIterator.hasNext()) {
                OO7_CompositePart compositePart = (OO7_CompositePart)compIterator.next();
                dfs( compositePart );
            } 
        } else {
            OO7_ComplexAssembly complexAssembly = (OO7_ComplexAssembly)anAssembly;
            Iterator aIterator = complexAssembly.subAssemblies().iterator();
            while (aIterator.hasNext()) {
                traversal( (OO7_Assembly)aIterator.next(), aTable );
            } 
        } 
    } 
    
    
    protected void dfs( OO7_CompositePart aPart ) throws Exception {
        Hashtable table = new Hashtable();
        dfsVisit( aPart.rootPart(), table );
        //System.out.println( "AtomicParts visited: " + table.size() );
    } 
    
    
    protected void dfsVisit( OO7_AtomicPart anAtomicPart, Hashtable aTable ) throws Exception {
        Iterator connIterator = anAtomicPart.from().iterator();
        while (connIterator.hasNext()) {
            OO7_Connection connection = (OO7_Connection)connIterator.next();
            OO7_AtomicPart part = connection.to();
            if (!aTable.containsKey( part )) {
                aTable.put( part, part );
                dfsVisit( part, aTable );
            } 
        } 
    } 
    
    
    public void matchQuery() throws Exception {
        int atomicParts = fNumAtomicPerComp[theScale] * fNumCompPerModule[theScale];
        long[] oids = new long[1000];
        int i;
        for (i = 0; i < oids.length; ++i) {
            oids[i] = getRandomInt( 0, atomicParts );
            //System.out.println( "oids[" + i + "] : " + oids[i] );
        } 
        long time = System.currentTimeMillis();
        for (i = 0; i < oids.length; ++i) {
            OO7_AtomicPart part = theModule.getAtomicPartByName("OO7_AtomicPart" + oids[i]);
        } 
        time = (System.currentTimeMillis() - time);
        System.out.println( "Millis: " + time );
    } 
    
    
    protected void createModule() throws Exception {
        OO7_CompositePart[] compositeParts = new OO7_CompositePart[fNumCompPerModule[theScale]];
        theModule = new OO7_ModuleImpl(getStorage());
        for (int i = 0; i < fNumCompPerModule[theScale]; ++i) {
            compositeParts[i] = createCompositePart();
        } 
        OO7_ComplexAssembly designRoot = 
                (OO7_ComplexAssembly)createAssembly( theModule, fNumAssmLevels[theScale], compositeParts );
        theModule.setDesignRoot( designRoot );
        modify();
    } 
    
    
    protected OO7_CompositePart createCompositePart() throws Exception {
        // Document erzeugen
        OO7_Document document = new OO7_DocumentImpl();
        // CompositeParterzeugen
        OO7_CompositePart compositePart = new OO7_CompositePartImpl(getStorage());
        if (verbose) {
            System.out.println( "CompositePart created" );
        } 
        compositePart.setDocumentation( document );
        
        OO7_AtomicPart[] atomicParts = new OO7_AtomicPart[fNumAtomicPerComp[theScale]];
        // AtomicParts erzeugen
        for (int i = 0; i < fNumAtomicPerComp[theScale]; ++i) {
            long oid = getAtomicPartOid();
            OO7_AtomicPart part = new OO7_AtomicPartImpl(getStorage());
            if (verbose) {
                System.out.println( "AtomicPart: " + oid + " created" );
            } 
            compositePart.addPart(part);
            part.setPartOf( compositePart );
            theModule.addAtomicPart("OO7_AtomicPart" + oid, part);
            atomicParts[i] = part;
        } 
        compositePart.setRootPart( atomicParts[0] );
        
        // AtomicParts miteinander verbinden
        for (int i = 0; i < fNumAtomicPerComp[theScale]; ++i) {
            int next = (i + 1) % fNumAtomicPerComp[theScale];
            OO7_Connection connection = new OO7_ConnectionImpl("");
            connection.setFrom( atomicParts[i] );
            atomicParts[i].addFrom( connection );
            connection.setTo( atomicParts[next] );
            atomicParts[next].addTo( connection );
            if (verbose) {
                System.out.println( "Connection: from: " + i + " to: " + next );
            } 
            for (int j = 0; j < (fConnPerAtomic[theScale] - 1); ++j) {
                next = getRandomInt( 0, fNumAtomicPerComp[theScale] );
                connection = new OO7_ConnectionImpl("");
                connection.setFrom( atomicParts[j] );
                atomicParts[j].addFrom( connection );
                connection.setTo( atomicParts[next] );
                atomicParts[next].addTo( connection );
                if (verbose) {
                    System.out.println( "Connection: from: " + j + " to: " + next );
                } 
            } 
        } 
        return compositePart;
    } 
    
    
    protected OO7_Assembly createAssembly( OO7_Module aModule, int aLevel, OO7_CompositePart[] someCompositeParts ) 
            throws Exception {
        if (verbose) {
            System.out.println( "level: " + aLevel );
        } 
        if (aLevel == 1) {
            OO7_BaseAssembly baseAssembly = new OO7_BaseAssemblyImpl(getStorage());
            aModule.addAssembly( baseAssembly );
            for (int j = 0; j < fNumCompPerAssm[theScale]; ++j) {
                int k = getRandomInt( 0, fNumCompPerModule[theScale] );
                baseAssembly.addComponentsShar( someCompositeParts[k] );
            } 
            return baseAssembly;
        } else {
            OO7_ComplexAssembly complexAssembly = new OO7_ComplexAssemblyImpl(getStorage());
            aModule.addAssembly( complexAssembly );
            for (int i = 0; i < fNumAssmPerAssm[theScale]; ++i) {
                complexAssembly.addSubAssembly( createAssembly( aModule, aLevel - 1, someCompositeParts ) );
            } 
            return complexAssembly;
        } 
    } 
}
