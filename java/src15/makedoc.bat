set SAVE_PATH=%PATH%
set PATH=\j2sdk1.5.0\bin;%path%
javadoc -source 1.5 -d ../doc15 -nodeprecated -nosince -public org/garret/perst/*.java
set PATH=%SAVE_PATH%