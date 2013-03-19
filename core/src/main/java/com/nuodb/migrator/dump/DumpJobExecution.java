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
package com.nuodb.migrator.dump;

import com.nuodb.migrator.jdbc.connection.ConnectionServices;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.job.JobExecution;
import com.nuodb.migrator.job.decorate.DecoratingJobExecution;
import com.nuodb.migrator.resultset.catalog.CatalogWriter;
import com.nuodb.migrator.resultset.format.value.ValueFormatRegistry;

import java.sql.Connection;

/**
 * @author Sergey Bushik
 */
public class DumpJobExecution extends DecoratingJobExecution {

    private static final String DATABASE = "database";
    private static final String DIALECT = "dialect";
    private static final String CATALOG_WRITER = "catalog.writer";
    private static final String CONNECTION = "connection";
    private static final String CONNECTION_SERVICES = "connection.services";
    private static final String VALUE_FORMAT_REGISTRY = "value.format.registry";

    public DumpJobExecution(JobExecution execution) {
        super(execution);
    }

    public Connection getConnection() {
        return (Connection) getContext().get(CONNECTION);
    }

    public void setConnection(Connection connection) {
        getContext().put(CONNECTION, connection);
    }

    public ConnectionServices getConnectionServices() {
        return (ConnectionServices) getContext().get(CONNECTION_SERVICES);
    }

    public void setConnectionServices(ConnectionServices connectionServices) {
        getContext().put(CONNECTION_SERVICES, connectionServices);
    }

    public Database getDatabase() {
        return (Database) getContext().get(DATABASE);
    }

    public void setDatabase(Database database) {
        getContext().put(DATABASE, database);
    }

    public Dialect getDialect() {
        return (Dialect) getContext().get(DIALECT);
    }

    public void setDialect(Dialect dialect) {
        getContext().put(DIALECT, dialect);
    }

    public CatalogWriter getCatalogWriter() {
        return (CatalogWriter) getContext().get(CATALOG_WRITER);
    }

    public void setCatalogWriter(CatalogWriter catalogWriter) {
        getContext().put(CATALOG_WRITER, catalogWriter);
    }

    public ValueFormatRegistry getValueFormatRegistry() {
        return (ValueFormatRegistry) getContext().get(VALUE_FORMAT_REGISTRY);
    }

    public void setValueFormatRegistry(ValueFormatRegistry valueFormatRegistry) {
        getContext().put(VALUE_FORMAT_REGISTRY, valueFormatRegistry);
    }
}
