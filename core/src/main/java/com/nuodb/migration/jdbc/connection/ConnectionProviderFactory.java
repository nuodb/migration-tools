/**
 * Copyright (c) 2012, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of NuoDB, Inc. nor the names of its contributors may
 *       be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.migration.jdbc.connection;

import com.nuodb.migration.MigrationException;
import com.nuodb.migration.spec.ConnectionSpec;
import com.nuodb.migration.spec.DriverConnectionSpec;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class ConnectionProviderFactory {

    private boolean pooling;
    private boolean logging;

    public ConnectionProvider createConnectionProvider(ConnectionSpec connectionSpec, boolean autoCommit) {
        if (connectionSpec == null) {
            return null;
        }
        ConnectionProvider connectionProvider;
        if (connectionSpec instanceof DriverConnectionSpec) {
            connectionProvider = createConnectionProvider((DriverConnectionSpec) connectionSpec, autoCommit);
        } else {
            throw new MigrationException(format("Connection specification is not supported %s", connectionSpec));
        }
        if (connectionProvider != null && logging) {
            connectionProvider = new StatementLoggingConnectionProvider(connectionProvider);
        }
        return connectionProvider;
    }

    protected ConnectionProvider createConnectionProvider(DriverConnectionSpec connectionSpec, boolean autoCommit) {
        if (connectionSpec.getUrl() == null) {
            return null;
        }
        if (isPooling()) {
            return new DriverPoolingConnectionProvider(connectionSpec, autoCommit);
        } else {
            return new DriverConnectionProvider(connectionSpec, autoCommit);
        }
    }

    public boolean isPooling() {
        return pooling;
    }

    public void setPooling(boolean pooling) {
        this.pooling = pooling;
    }

    public boolean isLogging() {
        return logging;
    }

    public void setLogging(boolean logging) {
        this.logging = logging;
    }
}
