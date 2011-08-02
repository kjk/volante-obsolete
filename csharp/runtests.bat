set save_path=%path%
set path=bin;%path%
set O=bin\dbg

%O%\Tests -fast

%O%\TestIndex2
%O%\TestEnumerator 200

%O%\TestCompoundIndex
%O%\TestRtree 20000
%O%\TestR2 20000
%O%\TestTtree
%O%\TestRaw
%O%\TestGC 20000
%O%\TestGC 20000 background
%O%\TestGC 20000 background altbtree
%O%\TestConcur
%O%\TestXML 20000
%O%\TestBackup
%O%\TestBlob
%O%\TestTimeSeries
%O%\TestBit 20000
%O%\TestList 100000

start %O%\TestReplic master
%O%\TestReplic slave
set path=%save_path%

