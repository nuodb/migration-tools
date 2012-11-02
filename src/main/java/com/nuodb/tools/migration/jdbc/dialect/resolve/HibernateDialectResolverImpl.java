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
package com.nuodb.tools.migration.jdbc.dialect.resolve;

import com.nuodb.tools.migration.jdbc.dialect.DatabaseDialect;
import com.nuodb.tools.migration.jdbc.dialect.DatabaseDialectException;
import com.nuodb.tools.migration.jdbc.dialect.DatabaseDialectResolver;
import org.hibernate.dialect.Dialect;
import org.hibernate.service.jdbc.dialect.internal.StandardDialectResolver;
import org.hibernate.service.jdbc.dialect.spi.DialectResolver;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Sergey Bushik
 */
public class HibernateDialectResolverImpl implements DatabaseDialectResolver {

    private final DialectResolver dialectResolver;

    public HibernateDialectResolverImpl() {
        this(new StandardDialectResolver());
    }

    public HibernateDialectResolverImpl(DialectResolver dialectResolver) {
        this.dialectResolver = dialectResolver;
    }

    @Override
    public DatabaseDialect resolve(DatabaseMetaData metaData) throws SQLException {
        return new HibernateDialect(dialectResolver.resolveDialect(metaData));
    }

    class HibernateDialect implements DatabaseDialect {

        private Dialect dialect;

        public HibernateDialect(Dialect dialect) {
            this.dialect = dialect;
        }

        @Override
        public char openQuote() {
            return dialect.openQuote();
        }

        @Override
        public char closeQuote() {
            return dialect.closeQuote();
        }

        @Override
        public String quote(String name) {
            return dialect.quote(name);
        }

        @Override
        public String getNoColumnsInsertString() {
            return dialect.getNoColumnsInsertString();
        }

        @Override
        public boolean supportsReadCatalogs() {
            throw new DatabaseDialectException("Feature is not supported");
        }

        @Override
        public boolean supportsReadSchemas() {
            throw new DatabaseDialectException("Feature is not supported");
        }

        @Override
        public void enableStreaming(Statement statement) throws SQLException {
            throw new DatabaseDialectException("Feature is not supported");
        }
    }
}
