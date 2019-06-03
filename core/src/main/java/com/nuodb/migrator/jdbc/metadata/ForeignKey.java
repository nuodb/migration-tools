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
package com.nuodb.migrator.jdbc.metadata;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import java.util.Collection;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newTreeMap;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.FOREIGN_KEY;

/**
 * @author Sergey Bushik
 */
public class ForeignKey extends ConstraintBase {

    private Map<Integer, ForeignKeyReference> references = newTreeMap();
    private ReferenceAction updateAction = ReferenceAction.NO_ACTION;
    private ReferenceAction deleteAction = ReferenceAction.NO_ACTION;
    private Deferrability deferrability;
    private Table primaryTable;
    private Table foreignTable;

    public ForeignKey() {
        super(FOREIGN_KEY);
    }

    public ForeignKey(String name) {
        super(FOREIGN_KEY, name);
    }

    public ForeignKey(Identifier name) {
        super(FOREIGN_KEY, name);
    }

    @Override
    public Collection<Column> getColumns() {
        return getForeignColumns();
    }

    public Table getPrimaryTable() {
        return primaryTable;
    }

    public void setPrimaryTable(Table primaryTable) {
        this.primaryTable = primaryTable;
    }

    public Table getForeignTable() {
        return foreignTable;
    }

    public void setForeignTable(Table foreignTable) {
        setTable(foreignTable);
        this.foreignTable = foreignTable;
    }

    public void addReference(Column primaryColumn, Column foreignColumn) {
        addReference(primaryColumn, foreignColumn, references.size());
    }

    public void addReference(Column primaryColumn, Column foreignColumn, int position) {
        references.put(position, new ForeignKeyReference(this, primaryColumn, foreignColumn, position));
    }

    public ReferenceAction getUpdateAction() {
        return updateAction;
    }

    public void setUpdateAction(ReferenceAction updateAction) {
        this.updateAction = updateAction;
    }

    public ReferenceAction getDeleteAction() {
        return deleteAction;
    }

    public void setDeleteAction(ReferenceAction deleteAction) {
        this.deleteAction = deleteAction;
    }

    public Deferrability getDeferrability() {
        return deferrability;
    }

    public void setDeferrability(Deferrability deferrability) {
        this.deferrability = deferrability;
    }

    public Collection<ForeignKeyReference> getReferences() {
        return newArrayList(references.values());
    }

    public Collection<Column> getPrimaryColumns() {
        return newArrayList(Collections2.transform(getReferences(), new Function<ForeignKeyReference, Column>() {
            @Override
            public Column apply(ForeignKeyReference input) {
                return input.getPrimaryColumn();
            }
        }));
    }

    public Collection<Column> getForeignColumns() {
        return newArrayList(Collections2.transform(getReferences(), new Function<ForeignKeyReference, Column>() {
            @Override
            public Column apply(ForeignKeyReference input) {
                return input.getForeignColumn();
            }
        }));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        ForeignKey that = (ForeignKey) o;

        if (deferrability != that.deferrability)
            return false;
        if (deleteAction != that.deleteAction)
            return false;
        if (updateAction != that.updateAction)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (updateAction != null ? updateAction.hashCode() : 0);
        result = 31 * result + (deleteAction != null ? deleteAction.hashCode() : 0);
        result = 31 * result + (deferrability != null ? deferrability.hashCode() : 0);
        return result;
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        super.output(indent, buffer);

        buffer.append(' ');
        output(indent, buffer, getReferences());
    }
}
