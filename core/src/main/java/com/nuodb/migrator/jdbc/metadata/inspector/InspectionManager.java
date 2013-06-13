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
import com.nuodb.migrator.jdbc.dialect.SimpleDialectResolver;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.resolve.DatabaseInfoUtils;
import com.nuodb.migrator.utils.SimplePriorityList;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migrator.jdbc.metadata.MetaDataType.*;

/**
 * Reads database meta data and creates meta model from it. Root meta model object is {@link Database} containing set of
 * catalogs, each catalog has a collection of schemas and schema is a wrapper of collection of a tables.
 *
 * @author Sergey Bushik
 */
public class InspectionManager {

    private Connection connection;
    private DialectResolver dialectResolver = new SimpleDialectResolver();
    private Collection<Inspector> inspectors = new SimplePriorityList<Inspector>();

    public InspectionManager() {
        addInspector(new SimpleDatabaseInspector());
        addInspector(new SimpleCatalogInspector());

        InspectorResolver schema = new InspectorResolver(SCHEMA, new SimpleSchemaInspector());
        schema.register(DatabaseInfoUtils.NUODB, new NuoDBSchemaInspector());
        schema.register(DatabaseInfoUtils.POSTGRE_SQL, new PostgreSQLSchemaInspector());
        schema.register(DatabaseInfoUtils.MSSQL_SERVER, new MSSQLServerSchemaInspector());
        addInspector(schema);

        InspectorResolver table = new InspectorResolver(TABLE, new SimpleTableInspector());
        table.register(DatabaseInfoUtils.NUODB, new NuoDBTableInspector());
        addInspector(table);

        InspectorResolver index = new InspectorResolver(INDEX, new SimpleIndexInspector());
        index.register(DatabaseInfoUtils.NUODB, new NuoDBIndexInspector());
        index.register(DatabaseInfoUtils.POSTGRE_SQL, new PostgreSQLIndexInspector());
        addInspector(index);

        InspectorResolver primaryKey = new InspectorResolver(PRIMARY_KEY, new SimplePrimaryKeyInspector());
        primaryKey.register(DatabaseInfoUtils.NUODB, new NuoDBPrimaryKeyInspector());
        addInspector(primaryKey);

        InspectorResolver foreignKey = new InspectorResolver(FOREIGN_KEY, new SimpleForeignKeyInspector());
        foreignKey.register(DatabaseInfoUtils.NUODB, new NuoDBForeignKeyInspector());
        addInspector(foreignKey);

        InspectorResolver column = new InspectorResolver(COLUMN, new SimpleColumnInspector());
        column.register(DatabaseInfoUtils.NUODB, new NuoDBColumnInspector());
        column.register(DatabaseInfoUtils.POSTGRE_SQL, new PostgreSQLColumnInspector());
        column.register(DatabaseInfoUtils.MSSQL_SERVER, new MSSQLServerColumnInspector());
        addInspector(column);

        InspectorResolver check = new InspectorResolver(CHECK);
        check.register(DatabaseInfoUtils.NUODB, new NuoDBCheckInspector());
        check.register(DatabaseInfoUtils.POSTGRE_SQL, new PostgreSQLCheckInspector());
        check.register(DatabaseInfoUtils.MSSQL_SERVER, new DB2CheckInspector());
        check.register(DatabaseInfoUtils.ORACLE, new OracleCheckInspector());
        check.register(DatabaseInfoUtils.DB2, new DB2CheckInspector());
        addInspector(check);

        InspectorResolver autoIncrement = new InspectorResolver(AUTO_INCREMENT);
        autoIncrement.register(DatabaseInfoUtils.MYSQL, new MySQLAutoIncrementInspector());
        autoIncrement.register(DatabaseInfoUtils.POSTGRE_SQL, new PostgreSQLAutoIncrementInspector());
        autoIncrement.register(DatabaseInfoUtils.MSSQL_SERVER, new MSSQLServerAutoIncrementInspector());
        autoIncrement.register(DatabaseInfoUtils.DB2, new DB2AutoIncrementInspector());
        addInspector(autoIncrement);
    }

    public InspectionResults inspect() throws SQLException {
        return inspect(MetaDataType.TYPES);
    }

    public InspectionResults inspect(MetaDataType... objectTypes) throws SQLException {
        return inspect(new TableInspectionScope(), objectTypes);
    }

    public InspectionResults inspect(InspectionScope inspectionScope, MetaDataType... objectTypes) throws SQLException {
        InspectionResults inspectionResults = createInspectionResults();
        inspect(inspectionResults, inspectionScope, objectTypes);
        return inspectionResults;
    }

    public InspectionResults inspect(MetaData object, MetaDataType... objectTypes) throws SQLException {
        InspectionResults inspectionResults = createInspectionResults();
        inspect(inspectionResults, object, objectTypes);
        return inspectionResults;
    }

    public InspectionResults inspect(Collection<MetaData> objects, MetaDataType... objectTypes) throws SQLException {
        InspectionResults inspectionResults = createInspectionResults();
        inspect(inspectionResults, objects, objectTypes);
        return inspectionResults;
    }

    public void inspect(InspectionResults inspectionResults, InspectionScope inspectionScope,
                        MetaDataType... objectTypes) throws SQLException {
        InspectionContext inspectionContext = createInspectionContext(inspectionResults, objectTypes);
        try {
            inspectionContext.inspect(inspectionScope, objectTypes);
        } finally {
            closeInspectionContext(inspectionContext);
        }
    }

    public void inspect(InspectionResults inspectionResults, MetaData object,
                        MetaDataType... objectTypes) throws SQLException {
        InspectionContext inspectionContext = createInspectionContext(inspectionResults, objectTypes);
        try {
            inspectionContext.inspect(object, objectTypes);
        } finally {
            closeInspectionContext(inspectionContext);
        }
    }

    public void inspect(InspectionResults inspectionResults, Collection<MetaData> objects,
                        MetaDataType... objectTypes) throws SQLException {
        InspectionContext inspectionContext = createInspectionContext(inspectionResults, objectTypes);
        try {
            inspectionContext.inspect(objects, objectTypes);
        } finally {
            closeInspectionContext(inspectionContext);
        }
    }

    protected InspectionResults createInspectionResults() {
        return new SimpleInspectionResults();
    }

    protected InspectionContext createInspectionContext(InspectionResults inspectionResults,
                                                        MetaDataType... objectTypes) {
        return new SimpleInspectionContext(this, inspectionResults, objectTypes);
    }

    protected void closeInspectionContext(InspectionContext inspectionContext) throws SQLException {
        inspectionContext.commit();
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public DialectResolver getDialectResolver() {
        return dialectResolver;
    }

    public void setDialectResolver(DialectResolver dialectResolver) {
        this.dialectResolver = dialectResolver;
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
}
