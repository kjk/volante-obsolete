del *.dbs
call TestIndex
del *.dbs
call TestIndex altbtree
del *.dbs
call TestIndex altbtree serializable
del *.dbs
call TestIndex inmemory
del *.dbs
call TestIndex2
call TestCompoundIndex
del *.dbs
call TestCompoundIndex altbtree
call TestMod
call TestIndexIterator
del *.dbs
call TestIndexIterator altbtree
call TestRtree
call TestR2
call TestTtree
call TestRaw
call TestRaw
call TestGC
del *.dbs
call TestGC background
del *.dbs
call TestGC altbtree background
del *.dbs
call TestConcur
call TestXML
call TestBackup
call TestBlob
call TestBlob
call TestTimeSeries
call TestBit
call TestThickIndex
call TestSet
start TestReplic master
call TestReplic slave
