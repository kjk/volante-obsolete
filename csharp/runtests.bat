@rem set save_path=%path%
@rem set path=bin;%path%
@rem set O=bin\dbg

%O%\Tests -fast

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

@rem start %O%\TestReplic master
@rem %O%\TestReplic slave

@rem set path=%save_path%

