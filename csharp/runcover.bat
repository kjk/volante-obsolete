set save_path=%path%
set path=bin;%path%;..\..\yepi\partcover

call "%ProgramFiles%\Microsoft Visual Studio 10.0\Common7\Tools\vsvars32.bat"
IF ERRORLEVEL 1 GOTO TRYX86
GOTO BUILD

:TRYX86
call "%ProgramFiles(x86)%\Microsoft Visual Studio 10.0\Common7\Tools\vsvars32.bat"
IF ERRORLEVEL 1 GOTO NEEDSVS

:BUILD
set O=bin\rel-cov
nmake -f makefile.msvc FOR_PARTCOVER=yes CFG=rel
IF ERRORLEVEL 1 GOTO BUILDFAILED

PartCover --target %O%\UnitTests.exe --include [NachoDB]* --include [UnitTests]* --output %O%\partcover.xml
IF ERRORLEVEL 1 GOTO PARTCOVERFAILED

python partcover-to-html.py %O%\partcover.xml cov
IF ERRORLEVEL 1 GOTO PARTCOVERTOHTMLFAILED

goto END

:PARTCOVERTOHTMLFAILED
echo partcover-to-html.py failed
goto END

:PARTCOVERFAILED
echo PartCover failed
goto END

:BUILDFAILED
echo Build failed
goto END

:NEEDSVS
echo Visual Studio 2010 doesn't seem to be installed
goto END

:END
set path=%save_path%


