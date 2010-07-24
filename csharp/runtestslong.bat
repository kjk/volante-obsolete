set save_path=%path%
set path=bin;%path%
set O=bin\dbg

%O%\UnitTestsRunner

%O%\TestIndex
%O%\TestIndex altbtree
%O%\TestIndex altbtree serializable
%O%\TestIndex inmemory

%O%\TestIndex2
%O%\TestEnumerator 1000

%O%\TestCompoundIndex
%O%\TestRtree 100000
%O%\TestR2 1000000
%O%\TestTtree
%O%\TestRaw
%O%\TestGC 1000000
%O%\TestGC 1000000 background
%O%\TestGC 1000000 background altbtree
%O%\TestConcur
%O%\TestXML 100000
%O%\TestBackup
%O%\TestBlob
%O%\TestTimeSeries
%O%\TestBit 100000
%O%\TestList 1000000

start %O%\TestReplic master
%O%\TestReplic slave
set path=%save_path%

