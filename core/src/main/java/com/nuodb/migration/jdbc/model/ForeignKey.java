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
package com.nuodb.migration.jdbc.model;

import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class ForeignKey extends HasIdentifierBase {

    private Map<Column, Column> references = Maps.newHashMap();
    private ForeignKeyRule updateRule = ForeignKeyRule.NO_ACTION;
    private ForeignKeyRule deleteRule = ForeignKeyRule.NO_ACTION;
    private ForeignKeyDeferrability deferrability;
    private Map<Column, String> sourceColumnKeys = Maps.newLinkedHashMap();
    private Map<Column, String> targetColumnKeys = Maps.newLinkedHashMap();
    private Map<Integer, Column> targetColumns = Maps.newTreeMap();

    public ForeignKey(Identifier name) {
        super(name);
    }

    public void addReference(Column primaryKeyColumn, Column foreignKeyColumn) {
        references.put(primaryKeyColumn, foreignKeyColumn);
    }

    public ForeignKeyRule getUpdateRule() {
        return updateRule;
    }

    public void setUpdateRule(ForeignKeyRule updateRule) {
        this.updateRule = updateRule;
    }

    public ForeignKeyRule getDeleteRule() {
        return deleteRule;
    }

    public void setDeleteRule(ForeignKeyRule deleteRule) {
        this.deleteRule = deleteRule;
    }

    public ForeignKeyDeferrability getDeferrability() {
        return deferrability;
    }

    public void setDeferrability(ForeignKeyDeferrability deferrability) {
        this.deferrability = deferrability;
    }

    public void addReference(Column sourceColumn, String sourceKeyName,
                             Column targetColumn, String targetKeyName, int sequence) {
        sourceColumnKeys.put(sourceColumn, sourceKeyName);
        targetColumnKeys.put(targetColumn, targetKeyName);
        targetColumns.put(sequence, targetColumn);
    }

    public Collection<Column> getSourceColumnKeys() {
        return sourceColumnKeys.keySet();
    }

    public Collection<Column> getTargetColumnKeys() {
        return targetColumns.values();
    }
}
