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
package com.nuodb.migration.jdbc.metadata.inspector;

import com.nuodb.migration.jdbc.metadata.Database;
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import com.nuodb.migration.jdbc.resolve.DatabaseAwareObjectResolver;
import com.nuodb.migration.jdbc.resolve.DatabaseMatcher;
import com.nuodb.migration.jdbc.resolve.SimpleDatabaseAwareObjectResolver;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class MetaDataReaderResolver extends MetaDataReaderBase implements DatabaseAwareObjectResolver<MetaDataReader> {

    private DatabaseAwareObjectResolver<MetaDataReader> databaseObjectReaderResolver;

    public MetaDataReaderResolver(MetaDataType metaDataType) {
        this(metaDataType, MetaDataReader.class);
    }

    public MetaDataReaderResolver(MetaDataType metaDataType,
                                  Class<? extends MetaDataReader> targetReaderClass) {
        this(metaDataType, new SimpleDatabaseAwareObjectResolver<MetaDataReader>(targetReaderClass));
    }

    public MetaDataReaderResolver(MetaDataType metaDataType,
                                  Class<? extends MetaDataReader> targetReaderClass,
                                  Class<? extends MetaDataReader> defaultReaderClass) {
        this(metaDataType, new SimpleDatabaseAwareObjectResolver<MetaDataReader>(
                targetReaderClass, defaultReaderClass));
    }

    public MetaDataReaderResolver(MetaDataType metaDataType,
                                  DatabaseAwareObjectResolver<MetaDataReader> databaseObjectReaderResolver) {
        super(metaDataType);
        this.databaseObjectReaderResolver = databaseObjectReaderResolver;
    }

    @Override
    public void read(DatabaseInspector inspector, Database database,
                     DatabaseMetaData metaData) throws SQLException {
        MetaDataReader metaDataReader = databaseObjectReaderResolver.resolveObject(metaData);
        if (metaDataReader != null) {
            metaDataReader.read(inspector, database, metaData);
        }
    }

    public void registerObject(String productName, Class<? extends MetaDataReader> readerClass) {
        databaseObjectReaderResolver.registerObject(productName, readerClass);
    }

    public void registerObject(String productName, String productVersion,
                               Class<? extends MetaDataReader> readerClass) {
        databaseObjectReaderResolver.registerObject(productName, productVersion, readerClass);
    }

    public void registerObject(String productName, String productVersion, int majorVersion,
                               Class<? extends MetaDataReader> readerClass) {
        databaseObjectReaderResolver.registerObject(productName, productVersion, majorVersion, readerClass);
    }

    public void registerObject(String productName, String productVersion, int majorVersion, int minorVersion,
                               Class<? extends MetaDataReader> readerClass) {
        databaseObjectReaderResolver.registerObject(productName, productVersion, majorVersion, minorVersion, readerClass);
    }

    public void registerObject(DatabaseMatcher databaseMatcher, Class<? extends MetaDataReader> readerClass) {
        databaseObjectReaderResolver.registerObject(databaseMatcher, readerClass);
    }

    @Override
    public MetaDataReader resolveObject(DatabaseMetaData metaData) throws SQLException {
        return databaseObjectReaderResolver.resolveObject(metaData);
    }
}
