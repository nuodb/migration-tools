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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.Table;

import java.sql.SQLException;
import java.util.Collection;

import static java.util.Collections.singleton;

/**
 * @author Sergey Bushik
 */
public abstract class TableInspectorBase<M extends MetaData, T extends TableInspectionScope> extends InspectorBase<M, T> {

    public TableInspectorBase(MetaDataType objectType) {
        this(objectType, (Class<? extends T>) TableInspectionScope.class);
    }

    public TableInspectorBase(MetaDataType objectType, Class<? extends T> inspectionScopeClass) {
        super(objectType, MetaDataType.TABLE, inspectionScopeClass);
    }

    protected TableInspectorBase(MetaDataType objectType, MetaDataType enclosingObjectType,
                                 Class<? extends InspectionScope> inspectionScopeClass) {
        super(objectType, enclosingObjectType, inspectionScopeClass);
    }

    @Override
    public void inspectObjects(InspectionContext inspectionContext, Collection<? extends M> objects) throws SQLException {
        inspectScopes(inspectionContext, createInspectionScopes(objects));
    }

    @Override
    public void inspectScope(InspectionContext inspectionContext, T inspectionScope) throws SQLException {
        inspectScopes(inspectionContext, singleton(inspectionScope));
    }

    protected abstract Collection<? extends T> createInspectionScopes(Collection<? extends M> objects);

    protected abstract void inspectScopes(InspectionContext inspectionContext,
                                          Collection<? extends T> inspectionScopes) throws SQLException;

    @Override
    public boolean supports(InspectionContext inspectionContext, InspectionScope inspectionScope) {
        return inspectionScope instanceof TableInspectionScope && supports((TableInspectionScope) inspectionScope);
    }

    protected boolean supports(TableInspectionScope inspectionScope) {
        return true;
    }

    public static TableInspectionScope createTableInspectionScope(Table table) {
        return new TableInspectionScope(
                table.getCatalog().getName(), table.getSchema().getName(), table.getName());
    }

    public static Collection<TableInspectionScope> createTableInspectionScopes(Collection<? extends Table> tables) {
        return Lists.newArrayList(
                Iterables.transform(tables, new Function<Table, TableInspectionScope>() {
                    @Override
                    public TableInspectionScope apply(Table table) {
                        return createTableInspectionScope(table);
                    }
                }));
    }
}
