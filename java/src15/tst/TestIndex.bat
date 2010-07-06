set SAVE_PATH=%PATH%
set PATH=\j2sdk1.5.0\bin;%path%
java -classpath .;..\..\lib\perst15.jar TestIndex
set PATH=%SAVE_PATH%