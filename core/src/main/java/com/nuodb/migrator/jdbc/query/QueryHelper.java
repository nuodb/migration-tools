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
package com.nuodb.migrator.jdbc.query;

import com.nuodb.migrator.jdbc.dialect.Dialect;

import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.nuodb.migrator.utils.StringUtils.indexOf;
import static java.util.Collections.singleton;
import static java.util.regex.Pattern.compile;
import static java.util.regex.Pattern.quote;
import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.substring;

/**
 * @author Sergey Bushik
 */
public class QueryHelper {

    public static final String SELECT = "SELECT";
    public static final String DISTINCT = "DISTINCT";
    public static final String FROM = "FROM";

    private Dialect dialect;

    public QueryHelper(Dialect dialect) {
        this.dialect = dialect;
    }

    public String getColumns(String query) {
        Map<String, String> columnAliases = getColumnAliases(query);
        boolean multiple = false;
        Collection<String> aliases = newArrayList();
        for (Map.Entry<String, String> columnAlias : columnAliases.entrySet()) {
            if (isMultiple(columnAlias.getKey())) {
                multiple = true;
            } else {
                aliases.add(columnAlias.getValue());
            }
        }
        return join(multiple ? singleton("*") : aliases, ", ");
    }

    public Map<String, String> getColumnAliases(String query) {
        int selectStart = indexOf(query, SELECT, 0, true);
        int fromStart = indexOf(query, FROM, selectStart);
        String columnList = substring(query, selectStart + SELECT.length(), fromStart);
        Map<String, String> columns = newLinkedHashMap();
        int startColumn;
        int startComma = 0;
        do {
            startColumn = startComma;
            startComma = indexOf(columnList, ",", startColumn, true, true);
            int endColumn = startComma == -1 ? columnList.length() : startComma;

            String column = columnList.substring(startColumn, endColumn).trim();
            columns.put(column, getAlias(column));
        } while (startComma++ > 0);
        return columns;
    }

    public String addColumn(String query, String column) {
        StringBuilder extendedQuery = new StringBuilder(query);
        int startSelect = indexOf(query, SELECT, 0, true);
        int startFrom = indexOf(query, FROM, startSelect);
        int startColumns = startSelect + SELECT.length();
        if (startColumns != -1 && startFrom != -1) {
            Map<String, String> columnAliases = getColumnAliases(query);
            columnAliases.put(column, getAlias(column));

            StringBuilder columns = new StringBuilder();
            columns.append(" ").append(join(columnAliases.keySet(), ", ")).append(" ");

            extendedQuery.replace(startColumns, startFrom, columns.toString());
        }
        return extendedQuery.toString();
    }

    protected String getAlias(String expression) {
        String openQuote = quote(dialect.openQuote());
        String closeQuote = quote(dialect.closeQuote());
        Matcher matcher = compile("\\s*(?i:AS)?\\s*((?:" + openQuote + ")?(\\w*)(?:" + closeQuote + ")?)$")
                .matcher(expression);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return expression;
    }

    protected boolean isMultiple(String expression) {
        return "*".equals(expression) || expression.endsWith(".*");
    }

    public Dialect getDialect() {
        return dialect;
    }
}
