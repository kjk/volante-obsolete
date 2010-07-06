del src\org\garret\perst\*.class
del src\org\garret\perst\impl\*.class
del src\org\garret\perst\impl\sun14\*.class
del src\org\garret\perst\aspectj\*.class
del src\org\garret\perst\jassist\*.class
del src15\org\garret\perst\*.class
del src15\org\garret\perst\impl\*.class
del src15\tst\*.class
del src15\tst\*.dbs
del tst\OO7\*.class
del tst\OO7\*.dbs
del tst\*.class
del tst\*.dbs
del tst\*.xml
del tst\aspectj\*.dbs
del tst\aspectj\*.class
del tst\jassist\*.dbs
del tst\jassist\*.class
del lib\perst_aspectj.jar
rd /s/q classes
cd ..
del perst.zip
zip -r perst.zip perst