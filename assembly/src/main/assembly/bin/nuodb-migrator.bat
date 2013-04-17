@echo off
@REM Copyright (c) 2012, NuoDB, Inc.
@REM All rights reserved.
@REM
@REM Redistribution and use in source and binary forms, with or without
@REM modification, are permitted provided that the following conditions are met:
@REM
@REM     * Redistributions of source code must retain the above copyright
@REM       notice, this list of conditions and the following disclaimer.
@REM     * Redistributions in binary form must reproduce the above copyright
@REM       notice, this list of conditions and the following disclaimer in the
@REM       documentation and/or other materials provided with the distribution.
@REM     * Neither the name of NuoDB, Inc. nor the names of its contributors may
@REM       be used to endorse or promote products derived from this software
@REM       without specific prior written permission.
@REM
@REM THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
@REM ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
@REM WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
@REM DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
@REM INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
@REM LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
@REM OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
@REM LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
@REM OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
@REM ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

@REM JAVA_HOME can optionally be set here

if exist "%JAVA_HOME%" goto okJavaHome
echo The JAVA_HOME variable must be set to a Java installation!
goto FAIL

:okJavaHome

@REM NUODB_ROOT is set here
if exist "%NUODB_ROOT%" goto okNuoDBHome
set NUODB_ROOT=C:\Program Files\NuoDB
if exist "%NUODB_ROOT%" goto okNuoDBHome
set NUODB_ROOT=C:\Program Files (x86)\NuoDB
if exist "%NUODB_ROOT%" goto okNuoDBHome

echo Cannot locate the NuoDB installation directory!
echo Set NUODB_ROOT appropriately.
goto FAIL

:okNuoDBHome

@REM Maximum heap size
set MAX_HEAP_SIZE=256M

if exist "%NUODB_MIGRATOR_ROOT%\jar" goto okHome
set NUODB_MIGRATOR_ROOT=%~dp0\..
if exist "%NUODB_MIGRATOR_ROOT%\jar" goto okHome

echo Cannot locate the NuoDB Migrator installation directory!
echo Set NUODB_MIGRATOR_ROOT appropriately.
goto FAIL

:okHome
set CLASSPATH=%NUODB_MIGRATOR_ROOT%\jar\nuodb-migrator-bootstrap-${project.version}.jar;%CLASSPATH%

set JAVA_OPTS=-Xmx%MAX_HEAP_SIZE% "-Dnuodb.root=%NUODB_ROOT%" "-Dnuodb.migrator.root=%NUODB_MIGRATOR_ROOT%"

"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp "%CLASSPATH%" com.nuodb.migrator.bootstrap.Bootstrap %*

exit /b 0

:FAIL
pause
exit /b 1
