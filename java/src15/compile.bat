set SAVE_PATH=%PATH%
set PATH=\j2sdk1.5.0\bin;%path%
javac -source 1.5 -g org\garret\perst\*.java org\garret\perst\impl\*.java
jar cvf ..\lib\perst15.jar org\garret\perst\*.class org\garret\perst\impl\*.class
set PATH=%SAVE_PATH%