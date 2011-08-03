#!/bin/sh

# Runs all tests in th elong form
# First argument is the name of bin directory where the executables are

cd ${1}
mono Tests.exe -slow

#mono ${1}/TestReplic.exe master&
#mono ${1}/TestReplic.exe slave