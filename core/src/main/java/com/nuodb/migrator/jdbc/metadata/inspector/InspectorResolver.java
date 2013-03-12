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

import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataHandlerBase;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.resolve.DatabaseInfo;
import com.nuodb.migrator.jdbc.resolve.ServiceResolver;
import com.nuodb.migrator.jdbc.resolve.SimpleServiceResolver;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class InspectorResolver extends MetaDataHandlerBase implements Inspector {

    private ServiceResolver<Inspector> inspectorResolver;

    public InspectorResolver(MetaDataType metaDataType) {
        this(metaDataType, new SimpleServiceResolver<Inspector>());
    }

    public InspectorResolver(MetaDataType metaDataType, Inspector inspector) {
        this(metaDataType, new SimpleServiceResolver<Inspector>(inspector));
    }

    public InspectorResolver(MetaDataType metaDataType, ServiceResolver<Inspector> inspectorResolver) {
        super(metaDataType);
        this.inspectorResolver = inspectorResolver;
    }

    @Override
    public void inspect(InspectionContext inspectionContext) throws SQLException {
        Inspector inspector = resolve(inspectionContext);
        if (inspector != null) {
            inspector.inspect(inspectionContext);
        }
    }

    @Override
    public void inspectObject(InspectionContext inspectionContext, MetaData object) throws SQLException {
        Inspector inspector = resolve(inspectionContext);
        if (inspector != null) {
            inspector.inspectObject(inspectionContext, object);
        }
    }

    @Override
    public void inspectObjects(InspectionContext inspectionContext, Collection objects) throws SQLException {
        Inspector inspector = resolve(inspectionContext);
        if (inspector != null) {
            inspector.inspectObjects(inspectionContext, objects);
        }
    }

    @Override
    public void inspectScope(InspectionContext inspectionContext, InspectionScope inspectionScope) throws SQLException {
        Inspector inspector = resolve(inspectionContext);
        if (inspector != null) {
            inspector.inspectScope(inspectionContext, inspectionScope);
        }
    }

    @Override
    public boolean supports(InspectionContext inspectionContext, InspectionScope inspectionScope) throws SQLException {
        Inspector inspector = resolve(inspectionContext);
        return inspector != null && inspector.supports(inspectionContext, inspectionScope);
    }

    public void register(String productName, Inspector inspector) {
        inspectorResolver.register(productName, inspector);
    }

    public void register(DatabaseInfo databaseInfo, Inspector inspector) {
        inspectorResolver.register(databaseInfo, inspector);
    }

    public void register(String productName, Class<? extends Inspector> serviceClass) {
        inspectorResolver.register(productName, serviceClass);
    }

    public void register(DatabaseInfo databaseInfo, Class<? extends Inspector> serviceClass) {
        inspectorResolver.register(databaseInfo, serviceClass);
    }

    public Inspector resolve(DatabaseInfo databaseInfo) {
        return inspectorResolver.resolve(databaseInfo);
    }

    public Inspector resolve(DatabaseMetaData databaseMetaData) throws SQLException {
        return inspectorResolver.resolve(databaseMetaData);
    }

    public Inspector resolve(InspectionContext inspectionContext) throws SQLException {
        return resolve(inspectionContext.getConnection().getMetaData());
    }
}
