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

import com.google.common.base.Function;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataHandlerBase;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.query.ParameterizedQuery;
import com.nuodb.migrator.jdbc.query.Query;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.JdbcUtils.closeQuietly;
import static java.lang.String.format;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({ "unchecked", "RedundantCast", "UnusedParameters" })
public abstract class InspectorBase<M extends MetaData, I extends InspectionScope> extends MetaDataHandlerBase
        implements Inspector<M, I> {

    protected transient final Logger logger = getLogger(getClass());

    private MetaDataType parentObjectType;
    private Class<? extends InspectionScope> inspectionScopeClass;

    protected InspectorBase(Class<? extends MetaData> objectClass,
            Class<? extends InspectionScope> inspectionScopeClass) {
        super(objectClass);
        this.inspectionScopeClass = inspectionScopeClass;
    }

    protected InspectorBase(MetaDataType objectType, Class<? extends InspectionScope> inspectionScopeClass) {
        super(objectType);
        this.inspectionScopeClass = inspectionScopeClass;
    }

    protected InspectorBase(MetaDataType objectType, MetaDataType parentObjectType,
            Class<? extends InspectionScope> inspectionScopeClass) {
        super(objectType);
        this.parentObjectType = parentObjectType;
        this.inspectionScopeClass = inspectionScopeClass;
    }

    @Override
    public void inspect(InspectionContext inspectionContext) throws SQLException {
        Collection<M> objects = getParentObjects(inspectionContext);
        if (objects != null && !isEmpty(objects)) {
            inspectObjects(inspectionContext, objects);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("No parent objects to inspect in the current context");
            }
        }
    }

    protected Collection<M> getParentObjects(InspectionContext inspectionScope) throws SQLException {
        return getParentObjectType() != null
                ? (Collection<M>) inspectionScope.getInspectionResults().getObjects(getParentObjectType())
                : null;
    }

    @Override
    public void inspectObject(InspectionContext inspectionContext, M object) throws SQLException {
        inspectScope(inspectionContext, createInspectionScope(object));
    }

    @Override
    public void inspectObjects(InspectionContext inspectionContext, Collection<? extends M> objects)
            throws SQLException {
        inspectScopes(inspectionContext, createInspectionScopes(objects));
    }

    @Override
    public void inspectScopes(InspectionContext inspectionContext, Collection<? extends I> inspectionScopes)
            throws SQLException {
        inspectionContext = createInspectionContext(inspectionContext, inspectionScopes);
        try {
            for (I inspectionScope : inspectionScopes) {
                inspectScope(inspectionContext, inspectionScope);
            }
        } finally {
            closeInspectionContext(inspectionContext);
        }
    }

    protected InspectionContext createInspectionContext(InspectionContext inspectionContext,
            Collection<? extends I> inspectionScopes) throws SQLException {
        return inspectionContext;
    }

    @Override
    public void inspectScope(InspectionContext inspectionContext, I inspectionScope) throws SQLException {
        if (logger.isTraceEnabled()) {
            logger.trace(format("Inspecting objects from %s", inspectionScope));
        }
        Query query = createQuery(inspectionContext, inspectionScope);
        Statement statement = createStatement(inspectionContext, inspectionScope, query);
        try {
            processStatement(inspectionContext, inspectionScope, query, statement);
        } finally {
            closeStatement(inspectionContext, inspectionScope, query, statement);
        }
    }

    protected Query createQuery(InspectionContext inspectionContext, I inspectionScope) {
        return null;
    }

    protected void closeInspectionContext(InspectionContext inspectionContext) throws SQLException {
    }

    @Override
    public Statement createStatement(InspectionContext inspectionContext, I inspectionScope, Query query)
            throws SQLException {
        Statement statement = null;
        if (query instanceof ParameterizedQuery) {
            statement = createStatement(inspectionContext, inspectionScope, (ParameterizedQuery) query);
        } else if (query != null) {
            statement = createStatement(inspectionContext, inspectionScope);
        }
        return statement;
    }

    protected Statement createStatement(InspectionContext inspectionContext, I inspectionScope) throws SQLException {
        return inspectionContext.getConnection().createStatement();
    }

    protected Statement createStatement(InspectionContext inspectionContext, I inspectionScope,
            ParameterizedQuery query) throws SQLException {
        return createStatement(inspectionContext, query);
    }

    protected Statement createStatement(InspectionContext inspectionContext, ParameterizedQuery query)
            throws SQLException {
        return createStatement(inspectionContext.getConnection(), query);
    }

    protected Statement createStatement(Connection connection, ParameterizedQuery query) throws SQLException {
        return connection.prepareStatement(query.toString(), TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
    }

    protected Statement createStatement(InspectionContext inspectionContext) throws SQLException {
        return createStatement(inspectionContext.getConnection());
    }

    protected Statement createStatement(Connection connection) throws SQLException {
        return connection.createStatement();
    }

    protected void processStatement(InspectionContext inspectionContext, I inspectionScope, Query query,
            Statement statement) throws SQLException {
        ResultSet resultSet = openResultSet(inspectionContext, inspectionScope, query, statement);
        try {
            processResultSet(inspectionContext, inspectionScope, query, resultSet);
        } finally {
            closeResultSet(inspectionContext, inspectionScope, query, resultSet);
        }
    }

    @Override
    public void closeStatement(InspectionContext inspectionContext, I inspectionScope, Query query, Statement statement)
            throws SQLException {
        closeQuietly(statement);
    }

    @Override
    public ResultSet openResultSet(InspectionContext inspectionContext, I inspectionScope, Query query,
            Statement statement) throws SQLException {
        ResultSet resultSet;
        if (statement instanceof PreparedStatement) {
            resultSet = openResultSet(inspectionContext, inspectionScope, (PreparedStatement) statement, query);
        } else if (statement != null) {
            resultSet = openResultSet(inspectionContext, inspectionScope, (Statement) statement, query);
        } else {
            resultSet = openResultSet(inspectionContext, inspectionScope, query);
        }
        return resultSet;
    }

    protected ResultSet openResultSet(InspectionContext inspectionContext, I inspectionScope,
            PreparedStatement statement, Query query) throws SQLException {
        int index = 1;
        if (query instanceof ParameterizedQuery) {
            for (Object parameter : ((ParameterizedQuery) query).getParameters()) {
                statement.setObject(index++, parameter);
            }
        }
        return statement.executeQuery();
    }

    protected ResultSet openResultSet(InspectionContext inspectionContext, I inspectionScope, Statement statement,
            Query query) throws SQLException {
        return statement.executeQuery(query.toString());
    }

    protected ResultSet openResultSet(InspectionContext inspectionContext, I inspectionScope, Query query)
            throws SQLException {
        return openResultSet(inspectionContext, inspectionScope);
    }

    protected ResultSet openResultSet(InspectionContext inspectionContext, I inspectionScope) throws SQLException {
        return null;
    }

    protected void processResultSet(InspectionContext inspectionContext, I inspectionScope, Query query,
            ResultSet resultSet) throws SQLException {
        processResultSet(inspectionContext, inspectionScope, resultSet);
    }

    protected void processResultSet(InspectionContext inspectionContext, I inspectionScope, ResultSet resultSet)
            throws SQLException {
        processResultSet(inspectionContext, resultSet);
    }

    protected void processResultSet(InspectionContext inspectionContext, ResultSet resultSet) throws SQLException {
    }

    protected void closeResultSet(InspectionContext inspectionContext, I inspectionScope, Query query,
            ResultSet resultSet) throws SQLException {
        closeResultSet(inspectionContext, inspectionScope, resultSet);
    }

    protected void closeResultSet(InspectionContext inspectionContext, I inspectionScope, ResultSet resultSet)
            throws SQLException {
        closeResultSet(inspectionContext, resultSet);
    }

    protected void closeResultSet(InspectionContext inspectionContext, ResultSet resultSet) throws SQLException {
        closeQuietly(resultSet);
    }

    protected abstract I createInspectionScope(M object);

    protected Collection<? extends I> createInspectionScopes(Collection<? extends M> objects) {
        return newArrayList(transform(objects, new Function<M, I>() {
            @Override
            public I apply(M object) {
                return createInspectionScope(object);
            }
        }));
    }

    @Override
    public boolean supportsScope(InspectionContext inspectionContext, InspectionScope inspectionScope) {
        return inspectionScope != null && getInspectionScopeClass().isInstance(inspectionScope)
                && supportsScope((I) inspectionScope);
    }

    protected boolean supportsScope(I inspectionScope) {
        return true;
    }

    public MetaDataType getParentObjectType() {
        return parentObjectType;
    }

    public Class<? extends InspectionScope> getInspectionScopeClass() {
        return inspectionScopeClass;
    }
}
