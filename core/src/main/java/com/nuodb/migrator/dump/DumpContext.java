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

import com.nuodb.migrator.backup.catalog.CatalogManager;
import com.nuodb.migrator.backup.format.FormatFactory;
import com.nuodb.migrator.backup.format.csv.CsvAttributes;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistry;
import com.nuodb.migrator.jdbc.connection.ConnectionProvider;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.metadata.Database;

import java.sql.Connection;
import java.util.Map;
import java.util.TimeZone;

import static com.google.common.collect.Maps.newHashMap;
import static java.lang.Runtime.getRuntime;

/**
 * @author Sergey Bushik
 */
public class DumpContext {

    public static final int THREADS = getRuntime().availableProcessors();

    private String format = CsvAttributes.FORMAT;
    private int threads = THREADS;
    private Dialect dialect;
    private Database database;
    private TimeZone timeZone;
    private CatalogManager catalogManager;
    private Connection connection;
    private ConnectionProvider connectionProvider;
    private FormatFactory formatFactory;
    private ValueFormatRegistry valueFormatRegistry;
    private Map<String, Object> formatAttributes = newHashMap();

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    public Database getDatabase() {
        return database;
    }

    public void setDatabase(Database database) {
        this.database = database;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public void setCatalogManager(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public ConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }

    public void setConnectionProvider(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    public FormatFactory getFormatFactory() {
        return formatFactory;
    }

    public void setFormatFactory(FormatFactory formatFactory) {
        this.formatFactory = formatFactory;
    }

    public ValueFormatRegistry getValueFormatRegistry() {
        return valueFormatRegistry;
    }

    public void setValueFormatRegistry(ValueFormatRegistry valueFormatRegistry) {
        this.valueFormatRegistry = valueFormatRegistry;
    }

    public Map<String, Object> getFormatAttributes() {
        return formatAttributes;
    }

    public void setFormatAttributes(Map<String, Object> formatAttributes) {
        this.formatAttributes = formatAttributes;
    }
}
