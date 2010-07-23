set save_path=%path%
set path=bin;%path%
set O=bin\dbg

del *.dbs
%O%\UnitTestsRunner

del *.dbs
%O%\TestIndex

del *.dbs
%O%\TestIndex altbtree

del *.dbs
%O%\TestIndex altbtree serializable

del *.dbs
%O%\TestIndex inmemory
%O%\TestIndex2
%O%\TestEnumerator

del *.dbs
%O%\TestCompoundIndex
%O%\TestRtree
%O%\TestR2
%O%\TestTtree
%O%\TestRaw
%O%\TestRaw
%O%\TestGC
%O%\TestGC background
%O%\TestGC background altbtree
%O%\TestConcur
%O%\TestXML
%O%\TestBackup
%O%\TestBlob
%O%\TestTimeSeries
%O%\TestBit
%O%\TestList

start %O%\TestReplic master
%O%\TestReplic slave
set path=%save_path%

