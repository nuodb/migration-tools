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

import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.Table;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class TableInspectorBase<M extends MetaData, T extends TableInspectionScope>
        extends ManagedInspectorBase<M, T> {

    public TableInspectorBase(MetaDataType objectType) {
        this(objectType, (Class<? extends T>) TableInspectionScope.class);
    }

    public TableInspectorBase(MetaDataType objectType, Class<? extends T> inspectionScopeClass) {
        super(objectType, MetaDataType.TABLE, inspectionScopeClass);
    }

    protected TableInspectorBase(MetaDataType objectType, MetaDataType parentObjectType,
            Class<? extends InspectionScope> inspectionScopeClass) {
        super(objectType, parentObjectType, inspectionScopeClass);
    }

    protected T createInspectionScope(M object) {
        return (T) createTableInspectionScope((Table) object);
    }

    @Override
    public boolean supportsScope(InspectionContext inspectionContext, InspectionScope inspectionScope) {
        return inspectionScope instanceof TableInspectionScope && supportsScope((TableInspectionScope) inspectionScope);
    }

    protected boolean supportsScope(TableInspectionScope tableInspectionScope) {
        return true;
    }

    public static TableInspectionScope createTableInspectionScope(Table table) {
        return new TableInspectionScope(table.getCatalog().getName(), table.getSchema().getName(), table.getName());
    }
}
