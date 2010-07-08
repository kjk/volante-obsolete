set save_path=%path%
set path=bin;%path%
del *.dbs
tests\TestIndex\bin\debug\TestIndex
del *.dbs
tests\TestIndex\bin\debug\TestIndex altbtree
del *.dbs
tests\TestIndex\bin\debug\TestIndex altbtree serializable
del *.dbs
tests\TestIndex\bin\debug\TestIndex inmemory
tests\TestIndex2\bin\debug\TestIndex2
tests\TestEnumerator\bin\debug\TestEnumerator
del *.dbs
tests\TestEnumerator\bin\debug\TestEnumerator altbtree
tests\TestCompoundIndex\bin\debug\TestCompoundIndex
tests\TestRtree\bin\debug\TestRtree
tests\TestR2\bin\debug\TestR2
tests\TestTtree\bin\debug\TestTtree
tests\TestRaw\bin\debug\TestRaw
tests\TestRaw\bin\debug\TestRaw
tests\TestGC\bin\debug\TestGC
tests\TestGC\bin\debug\TestGC background
tests\TestGC\bin\debug\TestGC background altbtree
tests\TestConcur\bin\debug\TestConcur
tests\TestXML\bin\debug\TestXML
tests\TestBackup\bin\debug\TestBackup
tests\TestBlob\bin\debug\TestBlob
tests\TestBlob\bin\debug\TestBlob
tests\TestTimeSeries\bin\debug\TestTimeSeries
tests\TestBit\bin\debug\TestBit
tests\TestList\bin\debug\TestList
start tests\TestReplic\bin\debug\TestReplic master
tests\TestReplic\bin\debug\TestReplic slave
set path=%save_path%
