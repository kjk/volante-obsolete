@set save_path=%path%
@set path=bin;%path%
@set O=bin\dbg

%O%\Tests -fast

%O%\TestBackup

@rem start %O%\TestReplic master
@rem %O%\TestReplic slave

@set path=%save_path%
