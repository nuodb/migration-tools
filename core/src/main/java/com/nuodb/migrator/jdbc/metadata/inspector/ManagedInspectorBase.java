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
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.query.Query;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class ManagedInspectorBase<M extends MetaData, I extends InspectionScope> extends InspectorBase<M,
        I> implements ManagedInspector<M, I> {

    protected ManagedInspectorBase(Class<? extends MetaData> objectClass,
                                   Class<? extends InspectionScope> inspectionScopeClass) {
        super(objectClass, inspectionScopeClass);
    }

    protected ManagedInspectorBase(MetaDataType objectType,
                                   Class<? extends InspectionScope> inspectionScopeClass) {
        super(objectType, inspectionScopeClass);
    }

    protected ManagedInspectorBase(MetaDataType objectType, MetaDataType parentObjectType,
                                   Class<? extends InspectionScope> inspectionScopeClass) {
        super(objectType, parentObjectType, inspectionScopeClass);
    }

    @Override
    protected InspectionContext createInspectionContext(InspectionContext inspectionContext,
                                                        Collection<? extends I> inspectionScopes) throws SQLException {
        return new SimpleManagedInspectionContext<M, I>(this, inspectionContext);
    }

    @Override
    protected Statement createStatement(InspectionContext inspectionContext, I inspectionScope, Query query)
            throws SQLException {
        Statement statement;
        if (inspectionContext instanceof ManagedInspectionContext) {
            statement = ((ManagedInspectionContext)inspectionContext).createStatement(inspectionScope, query);
        } else {
            statement = super.createStatement(inspectionContext, inspectionScope, query);
        }
        return statement;
    }

    @Override
    public Statement createStatement(ManagedInspectionContext<I> managedInspectionContext, I inspectionScope,
                                     Query query) throws SQLException {
        return super.createStatement(managedInspectionContext, inspectionScope, query);
    }

    @Override
    protected void closeStatement(InspectionContext inspectionContext, I inspectionScope, Query query,
                                  Statement statement) throws SQLException {
        if (inspectionContext instanceof ManagedInspectionContext) {
            ((ManagedInspectionContext)inspectionContext).closeStatement(inspectionScope, query, statement);
        } else {
            super.closeStatement(inspectionContext, inspectionScope, query, statement);
        }
    }

    @Override
    protected void closeInspectionContext(InspectionContext inspectionContext) throws SQLException {
        if (inspectionContext instanceof ManagedInspectionContext) {
            ((ManagedInspectionContext) inspectionContext).closeStatements();
        } else {
            super.closeInspectionContext(inspectionContext);
        }
    }
}
