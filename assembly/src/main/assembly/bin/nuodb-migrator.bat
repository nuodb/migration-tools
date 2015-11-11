@ECHO OFF
@REM Copyright (c) 2015, NuoDB, Inc.
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

IF EXIST "%JAVA_HOME%" (SET "JAVA_EXEC=%JAVA_HOME%\bin\java") ELSE (SET "JAVA_EXEC=java")

@REM NUODB_HOME is set here
IF EXIST "%NUODB_HOME%" GOTO OK_NUODB_HOME
SET NUODB_HOME=C:\Program Files\NuoDB
IF EXIST "%NUODB_HOME%" GOTO OK_NUODB_HOME
SET NUODB_HOME=C:\Program Files (x86)\NuoDB
IF EXIST "%NUODB_HOME%" GOTO OK_NUODB_HOME

ECHO Cannot locate the NuoDB installation directory!
ECHO Set NUODB_HOME appropriately.
GOTO FAIL

:OK_NUODB_HOME

IF EXIST "%NUODB_MIGRATOR_LOG_DIR%" GOTO OK_NUODB_MIGRATOR_LOG_DIR
SET NUODB_MIGRATOR_LOG_DIR=%TEMP%

:OK_NUODB_MIGRATOR_LOG_DIR

IF EXIST "%NUODB_MIGRATOR_LOG_DATE_PATTERN%" GOTO OK_NUODB_MIGRATOR_LOG_DATE_PATTERN
SET NUODB_MIGRATOR_LOG_DATE_PATTERN=yyyy_MM_dd_HH_mm_ss

:OK_NUODB_MIGRATOR_LOG_DATE_PATTERN

@REM Maximum heap size
SET MAX_HEAP_SIZE=256M

IF EXIST "%NUODB_MIGRATOR_HOME%\jar" GOTO OK_NUODB_MIGRATOR_HOME
SET NUODB_MIGRATOR_HOME=%~dp0\..
IF EXIST "%NUODB_MIGRATOR_HOME%\jar" GOTO OK_NUODB_MIGRATOR_HOME

ECHO Cannot locate the NuoDB Migrator installation directory!
ECHO Set NUODB_MIGRATOR_HOME appropriately.
GOTO FAIL

:OK_NUODB_MIGRATOR_HOME

SETLOCAL
SET "JAVA_OPTS=%JAVA_OPTS% "-Xmx%MAX_HEAP_SIZE%""
SET "JAVA_OPTS=%JAVA_OPTS% "-Dnuodb.home=%NUODB_HOME%""
SET "JAVA_OPTS=%JAVA_OPTS% "-Dnuodb.migrator.home=%NUODB_MIGRATOR_HOME%""
SET "JAVA_OPTS=%JAVA_OPTS% "-Dnuodb.migrator.log.dir=%NUODB_MIGRATOR_LOG_DIR%""
SET "JAVA_OPTS=%JAVA_OPTS% "-Dnuodb.migrator.log.date.pattern=%NUODB_MIGRATOR_LOG_DATE_PATTERN%""

SET "CLASSPATH=%CLASSPATH%;%NUODB_MIGRATOR_HOME%\conf"
SET "CLASSPATH=%CLASSPATH%;%NUODB_MIGRATOR_HOME%\jar\slf4j-api-${slf4j.version}.jar"
SET "CLASSPATH=%CLASSPATH%;%NUODB_MIGRATOR_HOME%\jar\slf4j-log4j12-${slf4j.version}.jar"
SET "CLASSPATH=%CLASSPATH%;%NUODB_MIGRATOR_HOME%\jar\log4j-${log4j.version}.jar"
SET "CLASSPATH=%CLASSPATH%;%NUODB_MIGRATOR_HOME%\jar\nuodb-migrator-bootstrap-${project.version}.jar"

"%JAVA_EXEC%" %JAVA_OPTS% -cp "%CLASSPATH%" com.nuodb.migrator.bootstrap.Bootstrap %*
ENDLOCAL
EXIT /b %ERRORLEVEL%

:FAIL
PAUSE
EXIT /b 1
