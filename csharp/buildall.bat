call "%ProgramFiles%\Microsoft Visual Studio 10.0\Common7\Tools\vsvars32.bat"
IF ERRORLEVEL 1 GOTO TRYX86
GOTO BUILD

:TRYX86
call "%ProgramFiles(x86)%\Microsoft Visual Studio 10.0\Common7\Tools\vsvars32.bat"
IF ERRORLEVEL 1 GOTO NEEDSVS

:BUILD
nmake -f makefile.msvc CFG=dbg
IF ERRORLEVEL 1 GOTO COMPILE_FAILED

nmake -f makefile.msvc CFG=rel OMIT_XML=yes
IF ERRORLEVEL 1 GOTO COMPILE_FAILED

nmake -f makefile.msvc CFG=rel OMIT_REPLICATION=yes
IF ERRORLEVEL 1 GOTO COMPILE_FAILED

nmake -f makefile.msvc CFG=rel OMIT_RAW_TYPE=yes
IF ERRORLEVEL 1 GOTO COMPILE_FAILED

nmake -f makefile.msvc CFG=rel OMIT_ALL=yes
IF ERRORLEVEL 1 GOTO COMPILE_FAILED

nmake -f makefile.msvc CFG=rel
IF ERRORLEVEL 1 GOTO COMPILE_FAILED

goto END

:COMPILE_FAILED
echo Compilation failed

:NEEDSVS
echo Visual Studio 2010 doesn't seem to be installed

:END
