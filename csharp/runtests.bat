@set save_path=%path%
@set path=bin;%path%
@set O=bin\dbg

@rem cd %O%
%O%\Tests.exe -fast

@rem start %O%\TestReplic master
@rem %O%\TestReplic slave

@set path=%save_path%
