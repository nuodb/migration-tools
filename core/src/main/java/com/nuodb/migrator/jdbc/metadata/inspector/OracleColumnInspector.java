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

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.UserDefinedType;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.dialect.OracleDialect.INTERVAL_DAY_TO_SECOND_REGEX;
import static com.nuodb.migrator.jdbc.dialect.OracleDialect.INTERVAL_YEAR_TO_MATCH_REGEX;
import static com.nuodb.migrator.jdbc.metadata.DefaultValue.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.SCHEMA;
import static com.nuodb.migrator.jdbc.model.FieldFactory.newFieldList;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.*;

/**
 * Fixes "Stream has already been closed"
 * https://issues.apache.org/jira/browse/DDLUTILS-29
 *
 * @author Sergey Bushik
 */
public class OracleColumnInspector extends SimpleColumnInspector {

    private final transient Logger logger = LoggerFactory.getLogger(getClass());
    private static final long cSize = 24;

    /**
     * Fetches LONG or LONG RAW columns first, as these kind of columns are read
     * as stream, if not read in a proper order, there will be an error
     *
     * @param inspectionContext
     *            with inspection data
     * @param columns
     *            result set holding column attributes
     * @param column
     *            to populate from result set
     * @throws SQLException
     */
    @Override
    protected void processColumn(InspectionContext inspectionContext, ResultSet columns, Column column)
            throws SQLException {
        String defaultValue = trim(columns.getString("COLUMN_DEF"));
        if (startsWith(defaultValue, "'") && endsWith(defaultValue, "'")) {
            defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
        }
        JdbcTypeDesc typeDescAlias = inspectionContext.getDialect().getJdbcTypeAlias(columns.getInt("DATA_TYPE"),
                columns.getString("TYPE_NAME"));
        column.setTypeCode(typeDescAlias.getTypeCode());
        column.setTypeName(typeDescAlias.getTypeName());

        if (columns.getInt("DATA_TYPE") == Types.OTHER) {
            Schema schema = inspectionContext.getInspectionResults().getObject(SCHEMA);
            UserDefinedType userDefinedType = schema.getUserDefinedType(columns.getString("TYPE_NAME"));
            if (userDefinedType != null) {
                column.setTypeCode(columns.getInt("DATA_TYPE"));
                column.setTypeName(userDefinedType.getCode());
            }

            Collection<String> typeInfo = newArrayList();
            typeInfo.add(format("type name %s", columns.getString("TYPE_NAME")));
            typeInfo.add(format("type code %s", columns.getInt("DATA_TYPE")));
            typeInfo.add(format("length %d", column.getSize()));
            if (column.getPrecision() != null) {
                typeInfo.add(format("precision %d", column.getPrecision()));
            }
            if (column.getScale() != null) {
                typeInfo.add(format("scale %d", column.getScale()));
            }
            logger.warn(format("Unsupported type on table %s column %s: %s", column.getTable().getName(),
                    column.getName(), join(typeInfo, ", ")));
        }

        int columnSize = columns.getInt("COLUMN_SIZE");
        column.setSize((long) columnSize);
        if (columns.getInt("DATA_TYPE") == -104 || columns.getInt("DATA_TYPE") == -103
                || INTERVAL_DAY_TO_SECOND_REGEX.test(columns.getString("TYPE_NAME"))
                || INTERVAL_YEAR_TO_MATCH_REGEX.test(columns.getString("TYPE_NAME"))) {
            column.setSize(cSize);
        }
        column.setPrecision(columnSize);
        column.setScale(columns.getInt("DECIMAL_DIGITS"));

        column.setComment(columns.getString("REMARKS"));
        column.setPosition(columns.getInt("ORDINAL_POSITION"));
        String autoIncrement = newFieldList(columns.getMetaData()).get("IS_AUTOINCREMENT") != null
                ? columns.getString("IS_AUTOINCREMENT")
                : null;
        column.setAutoIncrement("YES".equals(autoIncrement));
        column.setNullable("YES".equals(columns.getString("IS_NULLABLE")));
        column.setDefaultValue(valueOf(defaultValue, true));
    }
}
