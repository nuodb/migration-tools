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

import com.google.common.collect.Iterables;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataHandlerBase;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;

import static java.util.Collections.singleton;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({"unchecked", "RedundantCast"})
public abstract class InspectorBase<M extends MetaData, I extends InspectionScope>
        extends MetaDataHandlerBase implements Inspector<M, I> {

    protected transient final Logger logger = LoggerFactory.getLogger(getClass());

    private Class<? extends InspectionScope> inspectionScopeClass;
    private MetaDataType parentObjectType;

    protected InspectorBase(Class<? extends MetaData> objectClass,
                            Class<? extends InspectionScope> inspectionScopeClass) {
        super(objectClass);
        this.inspectionScopeClass = inspectionScopeClass;
    }

    protected InspectorBase(MetaDataType objectType,
                            Class<? extends InspectionScope> inspectionScopeClass) {
        super(objectType);
        this.inspectionScopeClass = inspectionScopeClass;
    }

    protected InspectorBase(MetaDataType objectType, MetaDataType parentObjectType,
                            Class<? extends InspectionScope> inspectionScopeClass) {
        super(objectType);
        this.inspectionScopeClass = inspectionScopeClass;
        this.parentObjectType = parentObjectType;
    }

    @Override
    public void inspect(InspectionContext inspectionContext) throws SQLException {
        Collection<M> objects = getParentObjects(inspectionContext);
        if (objects != null && !Iterables.isEmpty(objects)) {
            inspectObjects(inspectionContext, objects);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("No parent objects to inspect in the current context");
            }
        }
    }

    @Override
    public void inspectObject(InspectionContext inspectionContext, M object) throws SQLException {
        inspectObjects(inspectionContext, singleton(object));
    }

    protected Collection<M> getParentObjects(InspectionContext inspectionScope) throws SQLException {
        return getParentObjectType() != null ?
                (Collection<M>) inspectionScope.getInspectionResults().getObjects(getParentObjectType()) : null;
    }

    public boolean supports(InspectionContext inspectionContext, InspectionScope inspectionScope) {
        return inspectionScope != null && inspectionScopeClass.isAssignableFrom(inspectionScope.getClass());
    }

    public MetaDataType getParentObjectType() {
        return parentObjectType;
    }
}
