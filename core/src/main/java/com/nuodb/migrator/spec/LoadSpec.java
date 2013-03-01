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
package com.nuodb.migrator.spec;

import com.google.common.collect.Maps;
import com.nuodb.migrator.jdbc.query.InsertType;

import java.util.Map;
import java.util.TimeZone;

/**
 * @author Sergey Bushik
 */
public class LoadSpec extends TaskSpecBase {

    private ConnectionSpec connectionSpec;
    private TimeZone timeZone;
    private ResourceSpec inputSpec;
    private InsertType insertType;
    private Map<String, InsertType> tableInsertTypes = Maps.newHashMap();

    public ConnectionSpec getConnectionSpec() {
        return connectionSpec;
    }

    public void setConnectionSpec(ConnectionSpec connectionSpec) {
        this.connectionSpec = connectionSpec;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public InsertType getInsertType() {
        return insertType;
    }

    public void setInsertType(InsertType insertType) {
        this.insertType = insertType;
    }

    public ResourceSpec getInputSpec() {
        return inputSpec;
    }

    public void setInputSpec(ResourceSpec inputSpec) {
        this.inputSpec = inputSpec;
    }

    public Map<String, InsertType> getTableInsertTypes() {
        return tableInsertTypes;
    }

    public void setTableInsertTypes(Map<String, InsertType> tableInsertTypes) {
        this.tableInsertTypes = tableInsertTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        LoadSpec loadSpec = (LoadSpec) o;

        if (connectionSpec != null ? !connectionSpec.equals(loadSpec.connectionSpec) : loadSpec.connectionSpec != null)
            return false;
        if (inputSpec != null ? !inputSpec.equals(loadSpec.inputSpec) : loadSpec.inputSpec != null) return false;
        if (insertType != loadSpec.insertType) return false;
        if (tableInsertTypes != null ? !tableInsertTypes.equals(
                loadSpec.tableInsertTypes) : loadSpec.tableInsertTypes != null) return false;
        if (timeZone != null ? !timeZone.equals(loadSpec.timeZone) : loadSpec.timeZone != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (connectionSpec != null ? connectionSpec.hashCode() : 0);
        result = 31 * result + (timeZone != null ? timeZone.hashCode() : 0);
        result = 31 * result + (inputSpec != null ? inputSpec.hashCode() : 0);
        result = 31 * result + (insertType != null ? insertType.hashCode() : 0);
        result = 31 * result + (tableInsertTypes != null ? tableInsertTypes.hashCode() : 0);
        return result;
    }
}
