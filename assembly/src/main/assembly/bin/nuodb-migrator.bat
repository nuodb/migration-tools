@ECHO OFF
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

IF EXIST "%JAVA_HOME%" GOTO OK_JAVA_HOME
ECHO The JAVA_HOME variable must be set to a Java installation!
GOTO FAIL

:OK_JAVA_HOME

@REM NUODB_ROOT is set here
IF EXIST "%NUODB_ROOT%" GOTO OK_NUODB_ROOT
SET NUODB_ROOT=C:\Program Files\NuoDB
IF EXIST "%NUODB_ROOT%" GOTO OK_NUODB_ROOT
SET NUODB_ROOT=C:\Program Files (x86)\NuoDB
IF EXIST "%NUODB_ROOT%" GOTO OK_NUODB_ROOT

ECHO Cannot locate the NuoDB installation directory!
ECHO Set NUODB_ROOT appropriately.
GOTO FAIL

:OK_NUODB_ROOT

@REM Maximum heap size
SET MAX_HEAP_SIZE=256M

IF EXIST "%NUODB_MIGRATOR_ROOT%\jar" GOTO OK_NUODB_MIGRATOR_ROOT
SET NUODB_MIGRATOR_ROOT=%~dp0\..
IF EXIST "%NUODB_MIGRATOR_ROOT%\jar" GOTO OK_NUODB_MIGRATOR_ROOT

ECHO Cannot locate the NuoDB Migrator installation directory!
ECHO Set NUODB_MIGRATOR_ROOT appropriately.
GOTO FAIL

:OK_NUODB_MIGRATOR_ROOT

SET "JAVA_OPTS=%JAVA_OPTS% -Xmx%MAX_HEAP_SIZE%"
SET "JAVA_OPTS=%JAVA_OPTS% -Dnuodb.root=%NUODB_ROOT%"
SET "JAVA_OPTS=%JAVA_OPTS% -Dnuodb.migrator.root=$%NUODB_MIGRATOR_ROOT%"

SET "CLASSPATH=%CLASSPATH%;%NUODB_MIGRATOR_ROOT%\conf"
SET "CLASSPATH=%CLASSPATH%;%NUODB_MIGRATOR_ROOT%\jar\slf4j-api-${slf4j.version}.jar"
SET "CLASSPATH=%CLASSPATH%;%NUODB_MIGRATOR_ROOT%\jar\slf4j-log4j12-${slf4j.version}.jar"
SET "CLASSPATH=%CLASSPATH%;%NUODB_MIGRATOR_ROOT%\jar\log4j-${log4j.version}.jar"
SET "CLASSPATH=%CLASSPATH%;%NUODB_MIGRATOR_ROOT%\jar\nuodb-migrator-bootstrap-${project.version}.jar"

"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp "%CLASSPATH%" com.nuodb.migrator.bootstrap.Bootstrap %*

EXIT /b 0

:FAIL
PAUSE
EXIT /b 1
