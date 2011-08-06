call "%ProgramFiles%\Microsoft Visual Studio 10.0\Common7\Tools\vsvars32.bat"
IF ERRORLEVEL 1 GOTO TRYX86
GOTO BUILD

:TRYX86
call "%ProgramFiles(x86)%\Microsoft Visual Studio 10.0\Common7\Tools\vsvars32.bat"
IF ERRORLEVEL 1 GOTO NEEDSVS

:BUILD
devenv Volante.sln /Project tests\Tests\Tests.csproj /ProjectConfig Release /Rebuild
IF ERRORLEVEL 1 GOTO FAILEDCOMPILE

bin\Release\Tests.exe -slow

@rem start %O%\TestReplic master
@rem %O%\TestReplic slave

goto END

:FAILEDCOMPILE
echo "Compilcation failed"
goto END

:NEEDSVS
echo Visual Studio 2010 doesn't seem to be installed

:END
