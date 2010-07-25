#!/bin/sh

# Runs all tests in the short form
# First argument is the name of bin directory where the executables are

rm -rf *.dbs
mono ${1}/UnitTestsRunner.exe

mono ${1}/TestIndex.exe
mono ${1}/TestIndex.exe altbtree
mono ${1}/TestIndex.exe altbtree serializable
mono ${1}/TestIndex.exe inmemory

mono ${1}/TestIndex2.exe
mono ${1}/TestEnumerator.exe 200

mono ${1}/TestCompoundIndex.exe
mono ${1}/TestRtree.exe 20000
mono ${1}/TestR2.exe 20000
mono ${1}/TestTtree.exe
mono ${1}/TestRaw.exe
mono ${1}/TestGC.exe 20000
mono ${1}/TestGC.exe 20000 background
mono ${1}/TestGC.exe 20000 background altbtree
mono ${1}/TestConcur.exe
mono ${1}/TestXML.exe 20000
mono ${1}/TestBackup.exe
mono ${1}/TestBlob.exe
mono ${1}/TestBlob.exe
mono ${1}/TestTimeSeries.exe
mono ${1}/TestBit.exe 20000
mono ${1}/TestList.exe 100000

mono ${1}/TestReplic.exe master&
mono ${1}/TestReplic.exe slave