javac -classpath .;..\lib\javassist.jar -g org\garret\perst\*.java org\garret\perst\impl\*.java org\garret\perst\jassist\*.java
jar cvf ..\lib\perst.jar org\garret\perst\*.class org\garret\perst\impl\*.class org\garret\perst\jassist\*.class
