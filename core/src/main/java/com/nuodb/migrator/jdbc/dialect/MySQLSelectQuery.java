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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.query.SelectQuery;

import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Lists.newArrayList;

/**
 * @author Sergey Bushik
 */
public class MySQLSelectQuery extends SelectQuery {

    private IndexHint indexHint;

    public IndexHint getIndexHint() {
        return indexHint;
    }

    public void setIndexHint(IndexHint indexHint) {
        this.indexHint = indexHint;
    }

    @Override
    public void append(StringBuilder query) {
        super.append(query);
        if (indexHint != null) {
            query.append(" ");
            query.append(indexHint.getIndexUsage());
            query.append(" INDEX (");
            for (Iterator<Index> iterator = indexHint.getIndexes().iterator(); iterator.hasNext();) {
                Index index = iterator.next();
                query.append(getDialect().getIdentifier(index.getName(), index));
                if (iterator.hasNext()) {
                    query.append(", ");
                }
            }
            query.append(")");
        }
    }

    public static class IndexHint {

        private IndexUsage indexUsage;
        private Collection<Index> indexes;

        public IndexHint(IndexUsage indexUsage, Index... indexes) {
            this(indexUsage, newArrayList(indexes));
        }

        public IndexHint(IndexUsage indexUsage, Collection<Index> indexes) {
            this.indexUsage = indexUsage;
            this.indexes = indexes;
        }

        public IndexUsage getIndexUsage() {
            return indexUsage;
        }

        public void setIndexUsage(IndexUsage indexUsage) {
            this.indexUsage = indexUsage;
        }

        public Collection<Index> getIndexes() {
            return indexes;
        }

        public void setIndexes(Collection<Index> indexes) {
            this.indexes = indexes;
        }
    }

    public static enum IndexUsage {
        FORCE, IGNORE, USE
    }
}
