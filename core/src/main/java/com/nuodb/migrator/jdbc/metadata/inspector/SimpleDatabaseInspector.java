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

import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.DriverInfo;
import com.nuodb.migrator.jdbc.metadata.MetaDataHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class SimpleDatabaseInspector extends MetaDataHandlerBase implements Inspector<Database, InspectionScope> {

    private transient final Logger logger = LoggerFactory.getLogger(getClass());

    public SimpleDatabaseInspector() {
        super(Database.class);
    }

    @Override
    public void inspect(InspectionContext inspectionContext) throws SQLException {
        Database database = InspectionResultsUtils.addDatabase(inspectionContext.getInspectionResults());
        DatabaseMetaData databaseMetaData = inspectionContext.getConnection().getMetaData();

        DriverInfo driverInfo = new DriverInfo();
        driverInfo.setName(databaseMetaData.getDriverName());
        driverInfo.setVersion(databaseMetaData.getDriverVersion());
        driverInfo.setMinorVersion(databaseMetaData.getDriverMinorVersion());
        driverInfo.setMajorVersion(databaseMetaData.getDriverMajorVersion());
        if (logger.isDebugEnabled()) {
            logger.debug(format("DriverInfo: %s", driverInfo));
        }
        database.setDriverInfo(driverInfo);

        DatabaseInfo databaseInfo = new DatabaseInfo();
        databaseInfo.setProductName(databaseMetaData.getDatabaseProductName());
        databaseInfo.setProductVersion(databaseMetaData.getDatabaseProductVersion());
        databaseInfo.setMinorVersion(databaseMetaData.getDatabaseMinorVersion());
        databaseInfo.setMajorVersion(databaseMetaData.getDatabaseMajorVersion());
        if (logger.isDebugEnabled()) {
            logger.debug(format("DatabaseInfo: %s", databaseInfo));
        }
        database.setDatabaseInfo(databaseInfo);

        database.setDialect(inspectionContext.getDialect());
    }

    @Override
    public void inspectObject(InspectionContext inspectionContext, Database database) throws SQLException {
        inspect(inspectionContext);
    }

    @Override
    public void inspectObjects(InspectionContext inspectionContext,
                               Collection<? extends Database> objects) throws SQLException {
        inspect(inspectionContext);
    }

    @Override
    public void inspectScope(InspectionContext inspectionContext, InspectionScope inspectionScope) throws SQLException {
        inspect(inspectionContext);
    }

    @Override
    public boolean supports(InspectionContext inspectionContext, InspectionScope inspectionScope) throws SQLException {
        return true;
    }
}
