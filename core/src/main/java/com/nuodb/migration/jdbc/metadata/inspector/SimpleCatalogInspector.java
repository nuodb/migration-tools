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
import com.nuodb.migration.jdbc.metadata.MetaDataHandlerBase;
import com.nuodb.migration.jdbc.metadata.MetaDataType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static com.nuodb.migration.jdbc.metadata.inspector.InspectionResultsUtils.addCatalog;

/**
 * @author Sergey Bushik
 */
public class SimpleCatalogInspector extends MetaDataHandlerBase implements Inspector<Database, InspectionScope> {

    public SimpleCatalogInspector() {
        super(MetaDataType.CATALOG);
    }

    @Override
    public void inspectScope(InspectionContext inspectionContext, InspectionScope inspectionScope) throws SQLException {
        inspect(inspectionContext);
    }

    @Override
    public void inspectObject(InspectionContext inspectionContext, Database object) throws SQLException {
        inspect(inspectionContext);
    }

    @Override
    public void inspectObjects(InspectionContext inspectionContext,
                               Collection<? extends Database> databases) throws SQLException {
        inspect(inspectionContext);
    }

    @Override
    public void inspect(InspectionContext inspectionContext) throws SQLException {
        InspectionResults inspectionResults = inspectionContext.getInspectionResults();
        ResultSet catalogs = inspectionContext.getConnection().getMetaData().getCatalogs();
        try {
            while (catalogs.next()) {
                addCatalog(inspectionResults, catalogs.getString("TABLE_CAT"));
            }
        } finally {
            close(catalogs);
        }
    }

    @Override
    public boolean supports(InspectionContext inspectionContext, InspectionScope inspectionScope) throws SQLException {
        return true;
    }
}
