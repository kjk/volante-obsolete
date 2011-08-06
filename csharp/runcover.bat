@set save_path=%path%
@set path=..\thirdparty\partcover;%path%

call "%ProgramFiles%\Microsoft Visual Studio 10.0\Common7\Tools\vsvars32.bat"
IF ERRORLEVEL 1 GOTO TRYX86
GOTO BUILD

:TRYX86
call "%ProgramFiles(x86)%\Microsoft Visual Studio 10.0\Common7\Tools\vsvars32.bat"
IF ERRORLEVEL 1 GOTO NEEDSVS

:BUILD
devenv Volante.sln /Project tests\Tests\Tests.csproj /ProjectConfig Partcover /Rebuild
@IF ERRORLEVEL 1 GOTO FAILEDCOMPILE

@set O=tests\Tests\bin\Partcover
@cd %O%
..\..\..\..\..\thirdparty\partcover\PartCover --target Tests.exe --include [Volante]* --include [Tests]* --output partcover.xml
@IF ERRORLEVEL 1 GOTO PARTCOVERFAILED
@cd ..\..\..\..

python partcover-to-html.py %O%\partcover.xml cov
@IF ERRORLEVEL 1 GOTO PARTCOVERTOHTMLFAILED

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
@echo Visual Studio 2010 doesn't seem to be installed
goto END

:END
@set path=%save_path%


