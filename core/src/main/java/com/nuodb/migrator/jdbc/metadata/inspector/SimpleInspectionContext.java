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

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newTreeSet;
import static com.nuodb.migrator.context.ContextUtils.createService;
import static com.nuodb.migrator.jdbc.metadata.MetaDataHandlerUtils.getHandler;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class SimpleInspectionContext implements InspectionContext {

    protected final transient Logger logger = getLogger(getClass());

    private Map<String, Object> attributes = newHashMap();
    private InspectionManager inspectionManager;
    private Connection connection;
    private InspectionResults inspectionResults;
    private MetaDataType[] objectTypes;
    private Dialect dialect;

    public SimpleInspectionContext(InspectionManager inspectionManager, Connection connection,
            InspectionResults inspectionResults, MetaDataType... objectTypes) {
        this.inspectionManager = inspectionManager;
        this.connection = connection;
        this.inspectionResults = inspectionResults;
        this.objectTypes = objectTypes;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    @Override
    public Dialect getDialect() throws SQLException {
        if (dialect == null) {
            DialectResolver dialectResolver = createService(inspectionManager.getDialectResolver(),
                    DialectResolver.class);
            dialect = dialectResolver.resolve(getConnection());
        }
        return dialect;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public InspectionResults getInspectionResults() {
        return inspectionResults;
    }

    @Override
    public void init() throws SQLException {
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
            if (logger.isDebugEnabled()) {
                logger.debug(format("Inspecting %s meta data", objectType));
            }
            inspector.inspectObject(this, object);
        }
    }

    @Override
    public void inspect(Collection<MetaData> objects, MetaDataType... objectTypes) throws SQLException {
        for (MetaDataType objectType : objectTypes) {
            Inspector inspector = findInspector(objectType);
            if (logger.isDebugEnabled()) {
                logger.debug(format("Inspecting %s meta data", objectType));
            }
            inspector.inspectObjects(this, objects);
        }
    }

    @Override
    public void inspect(InspectionScope scope, MetaDataType... objectTypes) throws SQLException {
        for (MetaDataType objectType : newTreeSet(asList(objectTypes))) {
            Inspector inspector = findInspector(objectType);
            if (logger.isDebugEnabled()) {
                logger.debug(format("Inspecting %s", objectType));
            }
            if (inspector.supportsScope(this, scope)) {
                inspector.inspectScope(this, scope);
            } else {
                inspector.inspect(this);
            }
        }
    }

    @Override
    public void close() throws SQLException {
        Connection connection = getConnection();
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
    }

    protected Inspector findInspector(MetaDataType objectType) {
        return getHandler(inspectionManager.getInspectors(), objectType);
    }
}
