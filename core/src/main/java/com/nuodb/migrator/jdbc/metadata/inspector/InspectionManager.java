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
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.resolve.DatabaseInfoUtils;
import com.nuodb.migrator.utils.SimplePriorityList;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.metadata.MetaDataType.*;
import static com.nuodb.migrator.jdbc.resolve.DatabaseInfoUtils.*;

/**
 * Reads database meta data and creates meta model from it. Root meta model object is {@link Database} containing set of
 * catalogs, each catalog has a collection of schemas and schema is a wrapper of collection of a tables.
 *
 * @author Sergey Bushik
 */
public class InspectionManager {

    private DialectResolver dialectResolver;
    private Collection<Inspector> inspectors = new SimplePriorityList<Inspector>();

    public InspectionManager() {
        addInspector(new SimpleDatabaseInspector());
        addInspector(new SimpleCatalogInspector());

        InspectorResolver schemaInspector = new InspectorResolver(SCHEMA, new SimpleSchemaInspector());
        schemaInspector.register(NUODB, new NuoDBSchemaInspector());
        schemaInspector.register(POSTGRE_SQL, new PostgreSQLSchemaInspector());
        schemaInspector.register(MSSQL_SERVER, new MSSQLServerSchemaInspector());
        addInspector(schemaInspector);

        InspectorResolver tableInspector = new InspectorResolver(TABLE, new SimpleTableInspector());
        tableInspector.register(NUODB, new NuoDBTableInspector());
        addInspector(tableInspector);

        InspectorResolver indexIndex = new InspectorResolver(INDEX, new SimpleIndexInspector());
        indexIndex.register(NUODB, new NuoDBIndexInspector());
        indexIndex.register(POSTGRE_SQL, new PostgreSQLIndexInspector());
        addInspector(indexIndex);

        InspectorResolver primaryKeyInspector = new InspectorResolver(PRIMARY_KEY, new SimplePrimaryKeyInspector());
        primaryKeyInspector.register(NUODB, new NuoDBPrimaryKeyInspector());
        addInspector(primaryKeyInspector);

        InspectorResolver foreignKeyInspector = new InspectorResolver(FOREIGN_KEY, new SimpleForeignKeyInspector());
        foreignKeyInspector.register(NUODB, new NuoDBForeignKeyInspector());
        addInspector(foreignKeyInspector);

        InspectorResolver columnInspector = new InspectorResolver(COLUMN, new SimpleColumnInspector());
        columnInspector.register(NUODB, new NuoDBColumnInspector());
        columnInspector.register(POSTGRE_SQL, new PostgreSQLColumnInspector());
        columnInspector.register(MSSQL_SERVER, new MSSQLServerColumnInspector());
        addInspector(columnInspector);

        InspectorResolver checkInspector = new InspectorResolver(CHECK);
        checkInspector.register(NUODB, new NuoDBCheckInspector());
        checkInspector.register(POSTGRE_SQL, new PostgreSQLCheckInspector());
        checkInspector.register(MSSQL_SERVER, new MSSQLServerCheckInspector());
        checkInspector.register(ORACLE, new OracleCheckInspector());
        checkInspector.register(DB2, new DB2CheckInspector());
        addInspector(checkInspector);

        InspectorResolver identityInspector = new InspectorResolver(IDENTITY);
        identityInspector.register(MYSQL, new MySQLIdentityInspector());
        identityInspector.register(POSTGRE_SQL, new PostgreSQLIdentityInspector());
        identityInspector.register(MSSQL_SERVER, new MSSQLServerIdentityInspector());
        identityInspector.register(DB2, new DB2IdentityInspector());
        addInspector(identityInspector);
    }

    public InspectionResults inspect(Connection connection) throws SQLException {
        return inspect(connection, MetaDataType.TYPES);
    }

    public InspectionResults inspect(Connection connection, MetaDataType... objectTypes) throws SQLException {
        return inspect(connection, new TableInspectionScope(), objectTypes);
    }

    public InspectionResults inspect(Connection connection, InspectionScope inspectionScope,
                                     MetaDataType... objectTypes) throws SQLException {
        InspectionResults inspectionResults = createInspectionResults();
        inspect(connection, inspectionResults, inspectionScope, objectTypes);
        return inspectionResults;
    }

    public InspectionResults inspect(Connection connection, MetaData object,
                                     MetaDataType... objectTypes) throws SQLException {
        InspectionResults inspectionResults = createInspectionResults();
        inspect(connection, inspectionResults, object, objectTypes);
        return inspectionResults;
    }

    public InspectionResults inspect(Connection connection, Collection<MetaData> objects,
                                     MetaDataType... objectTypes) throws SQLException {
        InspectionResults inspectionResults = createInspectionResults();
        inspect(connection, inspectionResults, objects, objectTypes);
        return inspectionResults;
    }

    public void inspect(Connection connection, InspectionResults inspectionResults, InspectionScope inspectionScope,
                        MetaDataType... objectTypes) throws SQLException {
        InspectionContext inspectionContext = createInspectionContext(connection, inspectionResults, objectTypes);
        try {
            inspectionContext.inspect(inspectionScope, objectTypes);
        } finally {
            closeInspectionContext(inspectionContext);
        }
    }

    public void inspect(Connection connection, InspectionResults inspectionResults, MetaData object,
                        MetaDataType... objectTypes) throws SQLException {
        InspectionContext inspectionContext = createInspectionContext(connection, inspectionResults, objectTypes);
        try {
            inspectionContext.inspect(object, objectTypes);
        } finally {
            closeInspectionContext(inspectionContext);
        }
    }

    public void inspect(Connection connection, InspectionResults inspectionResults, Collection<MetaData> objects,
                        MetaDataType... objectTypes) throws SQLException {
        InspectionContext inspectionContext = createInspectionContext(connection, inspectionResults, objectTypes);
        try {
            inspectionContext.inspect(objects, objectTypes);
        } finally {
            closeInspectionContext(inspectionContext);
        }
    }

    protected InspectionResults createInspectionResults() {
        return new SimpleInspectionResults();
    }

    protected InspectionContext createInspectionContext(Connection connection, InspectionResults inspectionResults,
                                                        MetaDataType... objectTypes) {
        return new SimpleInspectionContext(this, connection, inspectionResults, objectTypes);
    }

    protected void closeInspectionContext(InspectionContext inspectionContext) throws SQLException {
        inspectionContext.close();
    }

    public void addInspector(Inspector inspector) {
        inspectors.add(inspector);
    }

    public Collection<Inspector> getInspectors() {
        return inspectors;
    }

    public void setInspectors(Collection<Inspector> inspectors) {
        this.inspectors = inspectors;
    }

    public DialectResolver getDialectResolver() {
        return dialectResolver;
    }

    public void setDialectResolver(DialectResolver dialectResolver) {
        this.dialectResolver = dialectResolver;
    }
}
