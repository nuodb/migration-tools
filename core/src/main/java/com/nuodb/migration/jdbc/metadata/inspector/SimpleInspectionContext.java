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

import com.nuodb.migration.jdbc.dialect.Dialect;
import com.nuodb.migration.jdbc.dialect.DialectResolver;
import com.nuodb.migration.jdbc.metadata.MetaData;
import com.nuodb.migration.jdbc.metadata.MetaDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import static com.nuodb.migration.jdbc.metadata.MetaDataHandlerUtils.findMetaDataHandler;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class SimpleInspectionContext implements InspectionContext {

    protected final transient Logger logger = LoggerFactory.getLogger(getClass());

    private InspectionManager inspectionManager;
    private InspectionResults inspectionResults;
    private MetaDataType[] objectTypes;

    public SimpleInspectionContext(InspectionManager inspectionManager, InspectionResults inspectionResults,
                                   MetaDataType... objectTypes) {
        this.inspectionManager = inspectionManager;
        this.inspectionResults = inspectionResults;
        this.objectTypes = objectTypes;
    }

    @Override
    public Dialect getDialect() throws SQLException {
        DialectResolver dialectResolver = inspectionManager.getDialectResolver();
        return dialectResolver.resolve(getConnection());
    }

    @Override
    public Connection getConnection() throws SQLException {
        return inspectionManager.getConnection();
    }

    @Override
    public InspectionResults getInspectionResults() {
        return inspectionResults;
    }

    @Override
    public void setInspectionResults(InspectionResults inspectionResults) {
        this.inspectionResults = inspectionResults;
    }

    @Override
    public void inspect(MetaData object) throws SQLException {
        inspect(object, objectTypes);
    }

    @Override
    public void inspect(InspectionScope scope) throws SQLException {
        inspect(scope, objectTypes);
    }

    @Override
    public void inspect(MetaData object, MetaDataType... objectTypes) throws SQLException {
        for (MetaDataType objectType : objectTypes) {
            Inspector inspector = findInspector(objectType);
            inspector.inspectObject(this, object);
        }
    }

    @Override
    public void inspect(Collection<MetaData> objects, MetaDataType... objectTypes) throws SQLException {
        for (MetaDataType objectType : objectTypes) {
            Inspector inspector = findInspector(objectType);
            inspector.inspectObjects(this, objects);
        }
    }

    @Override
    public void inspect(InspectionScope scope, MetaDataType... objectTypes) throws SQLException {
        for (MetaDataType objectType : objectTypes) {
            Inspector inspector = findInspector(objectType);
            if (inspector.supports(this, scope)) {
                inspector.inspectScope(this, scope);
            } else {
                inspector.inspect(this);
            }
        }
    }

    protected Inspector findInspector(MetaDataType objectType) {
        if (logger.isDebugEnabled()) {
            logger.debug(format("Inspecting %s", objectType));
        }
        return findMetaDataHandler(inspectionManager.getInspectors(), objectType);
    }
}
