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

MAX_HEAP_SIZE="256M"

if not defined JAVA_HOME goto :error
if not defined NUODB_MIGRATION set NUODB_MIGRATION=%~dp0..

CLASSPATH=%NUODB_MIGRATION%/conf
for %%i in ("%NUODB_MIGRATION%\jar\*.jar") do set CLASSPATH=%CLASSPATH%;%%i

JVM_OPTIONS=-Xmx$%MAX_HEAP_SIZE%

"%JAVA_HOME%\bin\java" -ea -cp $CLASSPATH $JVM_OPTIONS com.nuodb.migration.cli.CliHandler %*
GOTO :finally

:error
echo The JAVA_HOME variable must be set to run this program!
pause

:finally
endlocal
