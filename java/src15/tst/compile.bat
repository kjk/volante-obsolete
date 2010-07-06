set SAVE_PATH=%PATH%
set PATH=\j2sdk1.5.0\bin;%path%
javac -source 1.5 -g -classpath ..\..\lib\perst15.jar;. *.java
set PATH=%SAVE_PATH%