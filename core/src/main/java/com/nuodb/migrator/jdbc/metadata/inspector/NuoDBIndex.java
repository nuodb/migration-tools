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

import com.nuodb.migrator.jdbc.query.QueryUtils;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;

/**
 * @author Sergey Bushik
 */
public class NuoDBIndex {

    public static final String QUERY =
            "SELECT * FROM SYSTEM.INDEXES\n" +
            "INNER JOIN SYSTEM.INDEXFIELDS ON INDEXES.SCHEMA=INDEXFIELDS.SCHEMA\n" +
            "AND INDEXES.TABLENAME=INDEXFIELDS.TABLENAME\n" +
            "AND INDEXES.INDEXNAME=INDEXFIELDS.INDEXNAME";

    public static final int PRIMARY_KEY = 0;
    public static final int UNIQUE = 1;
    public static final int KEY = 2;

    public static String createQuery(int... indexTypes) {
        final Collection<String> filters = newArrayList();
        filters.add("SCHEMA=?");
        filters.add("TABLENAME=?");
        if (indexTypes != null && indexTypes.length > 0) {
            if (indexTypes.length == 1) {
                filters.add("INDEXTYPE=" + indexTypes[0]);
            } else {
                StringBuilder filter = new StringBuilder();
                filter.append("INDEXTYPE IN (");
                for (int i = 0; i < indexTypes.length; i++) {
                    filter.append(indexTypes[i]);
                    if (i + 1 < indexTypes.length) {
                        filter.append(", ");
                    }
                }
                filter.append(")");
                filters.add(filter.toString());
            }
        }
        return QueryUtils.where(QUERY, filters, "AND");
    }
}
