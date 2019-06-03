/**
 * Copyright (c) 2015, NuoDB, Inc.
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
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.*;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.*;
import static com.nuodb.migrator.utils.Collections.newPrioritySet;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Reads database meta data and creates meta model from it. Root meta model
 * object is {@link Database} containing set of catalogs, each catalog has a
 * collection of schemas and schema is a wrapper of collection of a tables.
 *
 * @author Sergey Bushik
 */
public class InspectionManager {

    private final transient Logger logger = getLogger(getClass());
    private DialectResolver dialectResolver;
    private Collection<Inspector> inspectors = newPrioritySet();

    public InspectionManager() {
        InspectorResolver databaseInspector = new InspectorResolver(DATABASE, new SimpleDatabaseInspector());
        databaseInspector.register(NUODB, new NuoDBDatabaseInspector());
        addInspector(databaseInspector);

        addInspector(new SimpleCatalogInspector());

        InspectorResolver schemaInspector = new InspectorResolver(SCHEMA, new SimpleSchemaInspector());
        schemaInspector.register(NUODB, new NuoDBSchemaInspector());
        schemaInspector.register(POSTGRE_SQL, new PostgreSQLSchemaInspector());
        schemaInspector.register(MSSQL_SERVER, new MSSQLServerSchemaInspector());
        addInspector(schemaInspector);

        InspectorResolver userDefinedTypeInspector = new InspectorResolver(USER_DEFINED_TYPE,
                new SimpleUserDefinedTypeInspector());
        userDefinedTypeInspector.register(ORACLE, new OracleUserDefinedTypeTypeInspector());
        addInspector(userDefinedTypeInspector);

        InspectorResolver tableInspector = new InspectorResolver(TABLE, new SimpleTableInspector());
        tableInspector.register(NUODB, new NuoDBTableInspector());
        tableInspector.register(ORACLE, new OracleTableInspector());
        addInspector(tableInspector);

        InspectorResolver indexIndex = new InspectorResolver(INDEX, new SimpleIndexInspector());
        indexIndex.register(MYSQL, new MySQLIndexInspector());
        indexIndex.register(NUODB, new NuoDBIndexInspector());
        indexIndex.register(ORACLE, new OracleIndexInspector());
        indexIndex.register(POSTGRE_SQL, new PostgreSQLIndexInspector());
        indexIndex.register(POSTGRE_SQL_83, new PostgreSQL83IndexInspector());
        addInspector(indexIndex);

        InspectorResolver primaryKeyInspector = new InspectorResolver(PRIMARY_KEY, new SimplePrimaryKeyInspector());
        primaryKeyInspector.register(MYSQL, new MySQLPrimaryKeyInspector());
        primaryKeyInspector.register(NUODB, new NuoDBPrimaryKeyInspector());
        primaryKeyInspector.register(ORACLE, new OraclePrimaryKeyInspector());
        addInspector(primaryKeyInspector);

        InspectorResolver foreignKeyInspector = new InspectorResolver(FOREIGN_KEY, new SimpleForeignKeyInspector());
        foreignKeyInspector.register(NUODB, new NuoDBForeignKeyInspector());
        addInspector(foreignKeyInspector);

        InspectorResolver columnInspector = new InspectorResolver(COLUMN, new SimpleColumnInspector());
        columnInspector.register(MYSQL, new MySQLColumnInspector());
        columnInspector.register(NUODB, new NuoDBColumnInspector());
        columnInspector.register(POSTGRE_SQL, new PostgreSQLColumnInspector());
        columnInspector.register(MSSQL_SERVER, new MSSQLServerColumnInspector());
        columnInspector.register(ORACLE, new OracleColumnInspector());
        addInspector(columnInspector);

        InspectorResolver checkInspector = new InspectorResolver(CHECK);
        checkInspector.register(NUODB, new NuoDBCheckInspector());
        checkInspector.register(POSTGRE_SQL, new PostgreSQLCheckInspector());
        checkInspector.register(MSSQL_SERVER, new MSSQLServerCheckInspector());
        checkInspector.register(ORACLE, new OracleCheckInspector());
        checkInspector.register(DB2, new DB2CheckInspector());
        addInspector(checkInspector);

        InspectorResolver sequenceInspector = new InspectorResolver(SEQUENCE);
        sequenceInspector.register(ORACLE, new OracleSequenceInspector());
        sequenceInspector.register(MYSQL, new MySQLSequenceInspector());
        sequenceInspector.register(POSTGRE_SQL, new PostgreSQLSequenceInspector());
        sequenceInspector.register(MSSQL_SERVER, new MSSQLServerSequenceInspector());
        sequenceInspector.register(DB2, new DB2SequenceInspector());
        addInspector(sequenceInspector);

        InspectorResolver triggerResolver = new InspectorResolver(TRIGGER);
        addInspector(triggerResolver);

        InspectorResolver columnTriggerInspector = new InspectorResolver(COLUMN_TRIGGER);
        columnTriggerInspector.register(MYSQL, new MySQLColumnTriggerInspector());
        columnTriggerInspector.register(NUODB, new NuoDBColumnTriggerInspector());
        addInspector(columnTriggerInspector);
    }

    public InspectionResults inspect(Connection connection) throws SQLException {
        return inspect(connection, TYPES);
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

    public InspectionResults inspect(Connection connection, MetaData object, MetaDataType... objectTypes)
            throws SQLException {
        InspectionResults inspectionResults = createInspectionResults();
        inspect(connection, inspectionResults, object, objectTypes);
        return inspectionResults;
    }

    public InspectionResults inspect(Connection connection, Collection<MetaData> objects, MetaDataType... objectTypes)
            throws SQLException {
        InspectionResults inspectionResults = createInspectionResults();
        inspect(connection, inspectionResults, objects, objectTypes);
        return inspectionResults;
    }

    public void inspect(Connection connection, InspectionResults inspectionResults, InspectionScope inspectionScope,
            MetaDataType... objectTypes) throws SQLException {
        InspectionContext inspectionContext = createInspectionContext(connection, inspectionResults, objectTypes);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Inspecting objects %s", asList(objectTypes)));
            }
            inspectionContext.inspect(inspectionScope, objectTypes);
        } finally {
            closeInspectionContext(inspectionContext);
        }
    }

    public void inspect(Connection connection, InspectionResults inspectionResults, MetaData object,
            MetaDataType... objectTypes) throws SQLException {
        InspectionContext inspectionContext = createInspectionContext(connection, inspectionResults, objectTypes);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Inspecting objects %s", asList(objectTypes)));
            }
            inspectionContext.inspect(object, objectTypes);
        } finally {
            closeInspectionContext(inspectionContext);
        }
    }

    public void inspect(Connection connection, InspectionResults inspectionResults, Collection<MetaData> objects,
            MetaDataType... objectTypes) throws SQLException {
        InspectionContext inspectionContext = null;
        try {
            inspectionContext = createInspectionContext(connection, inspectionResults, objectTypes);
            if (logger.isDebugEnabled()) {
                logger.debug(format("Inspecting objects %s", asList(objectTypes)));
            }
            inspectionContext.inspect(objects, objectTypes);
        } finally {
            closeInspectionContext(inspectionContext);
        }
    }

    protected InspectionResults createInspectionResults() {
        return new SimpleInspectionResults();
    }

    protected InspectionContext createInspectionContext(Connection connection, InspectionResults inspectionResults,
            MetaDataType... objectTypes) throws SQLException {
        InspectionContext inspectionContext = new SimpleInspectionContext(this, connection, inspectionResults,
                objectTypes);
        inspectionContext.init();
        return inspectionContext;
    }

    protected void closeInspectionContext(InspectionContext inspectionContext) throws SQLException {
        if (inspectionContext != null) {
            inspectionContext.close();
        }
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
