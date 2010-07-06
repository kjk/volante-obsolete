set save_path=%path%
set path=bin;%path%
del *.dbs
TestIndex\bin\debug\TestIndex
del *.dbs
TestIndex\bin\debug\TestIndex altbtree
del *.dbs
TestIndex\bin\debug\TestIndex altbtree serializable
del *.dbs
TestIndex\bin\debug\TestIndex inmemory
TestIndex2\bin\debug\TestIndex2
TestEnumerator\bin\debug\TestEnumerator
del *.dbs
TestEnumerator\bin\debug\TestEnumerator altbtree
TestCompoundIndex\bin\debug\TestCompoundIndex
TestRtree\bin\debug\TestRtree
TestR2\bin\debug\TestR2
TestTtree\bin\debug\TestTtree
TestRaw\bin\debug\TestRaw
TestRaw\bin\debug\TestRaw
TestGC\bin\debug\TestGC
TestGC\bin\debug\TestGC background
TestGC\bin\debug\TestGC background altbtree
TestConcur\bin\debug\TestConcur
TestXML\bin\debug\TestXML
TestBackup\bin\debug\TestBackup
TestBlob\bin\debug\TestBlob
TestBlob\bin\debug\TestBlob
TestTimeSeries\bin\debug\TestTimeSeries
TestBit\bin\debug\TestBit
TestList\bin\debug\TestList
set path=%save_path%
