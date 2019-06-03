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
package com.nuodb.migrator.spec;

import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilter;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilterManager;

import java.util.Arrays;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.TYPES;
import static com.nuodb.migrator.jdbc.metadata.Table.TABLE;

/**
 * @author Sergey Bushik
 */
public class MetaDataSpec extends SpecBase {

    public static final Collection<MetaDataType> OBJECT_TYPES = newArrayList(TYPES);
    public static final String[] TABLE_TYPES = new String[] { TABLE };

    private Collection<MetaDataType> objectTypes = OBJECT_TYPES;
    private MetaDataFilterManager metaDataFilterManager = new MetaDataFilterManager();
    private String[] tableTypes = TABLE_TYPES;

    public Collection<MetaDataType> getObjectTypes() {
        return objectTypes;
    }

    public void setObjectTypes(Collection<MetaDataType> objectTypes) {
        this.objectTypes = objectTypes;
    }

    public MetaDataFilter getMetaDataFilter(MetaDataType objectType) {
        MetaDataFilterManager metaDataFilterManager = getMetaDataFilterManager();
        return metaDataFilterManager != null ? metaDataFilterManager.getMetaDataFilter(objectType) : null;
    }

    public MetaDataFilterManager getMetaDataFilterManager() {
        return metaDataFilterManager;
    }

    public void setMetaDataFilterManager(MetaDataFilterManager metaDataFilterManager) {
        this.metaDataFilterManager = metaDataFilterManager;
    }

    public String[] getTableTypes() {
        return tableTypes;
    }

    public void setTableTypes(String[] tableTypes) {
        this.tableTypes = tableTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        MetaDataSpec that = (MetaDataSpec) o;

        if (objectTypes != null ? !objectTypes.equals(that.objectTypes) : that.objectTypes != null)
            return false;
        if (metaDataFilterManager != null ? !metaDataFilterManager.equals(that.metaDataFilterManager)
                : that.metaDataFilterManager != null)
            return false;
        if (!Arrays.equals(tableTypes, that.tableTypes))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (objectTypes != null ? objectTypes.hashCode() : 0);
        result = 31 * result + (metaDataFilterManager != null ? metaDataFilterManager.hashCode() : 0);
        result = 31 * result + (tableTypes != null ? Arrays.hashCode(tableTypes) : 0);
        return result;
    }
}
