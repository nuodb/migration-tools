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
package com.nuodb.migration.jdbc.metadata.inspector;

import com.nuodb.migration.jdbc.dialect.Dialect;
import com.nuodb.migration.jdbc.metadata.*;
import com.nuodb.migration.jdbc.query.StatementCallback;
import com.nuodb.migration.jdbc.query.StatementCreator;
import com.nuodb.migration.jdbc.query.StatementTemplate;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class PostgreSQLAutoIncrementReader extends MetaDataReaderBase {

    public PostgreSQLAutoIncrementReader() {
        super(MetaDataType.AUTO_INCREMENT);
    }

    @Override
    public void read(DatabaseInspector inspector, Database database, DatabaseMetaData metaData) throws SQLException {
        Dialect dialect = database.getDialect();
        for (Table table : database.listTables()) {
            for (final Column column : table.getColumns()) {
                if (!column.isAutoIncrement()) {
                    continue;
                }
                String defaultValue = column.getDefaultValue();
                String[] split = StringUtils.split(defaultValue, '\'');
                if (split.length < 2) {
                    continue;
                }
                String sequence = split[1];
                final String query = format("SELECT * FROM %s.%s",
                        dialect.getIdentifier(table.getSchema().getName()),
                        dialect.getIdentifier(sequence));
                StatementTemplate template = new StatementTemplate(metaData.getConnection());
                template.execute(
                        new StatementCreator<PreparedStatement>() {
                            @Override
                            public PreparedStatement create(Connection connection) throws SQLException {
                                return connection.prepareStatement(query);
                            }
                        },
                        new StatementCallback<PreparedStatement>() {
                            @Override
                            public void execute(PreparedStatement statement) throws SQLException {
                                readSequence(column, statement.executeQuery());
                            }
                        }
                );
                column.setDefaultValue(null);
            }
        }
    }

    protected void readSequence(Column column, ResultSet resultSet) throws SQLException {
        if (resultSet.next()) {
            Sequence sequence = new Sequence();
            sequence.setName(resultSet.getString("SEQUENCE_NAME"));
            sequence.setLastValue(resultSet.getLong("LAST_VALUE"));
            sequence.setStartWith(resultSet.getLong("START_VALUE"));
            sequence.setMinValue(resultSet.getLong("MIN_VALUE"));
            sequence.setMaxValue(resultSet.getLong("MAX_VALUE"));
            sequence.setIncrementBy(resultSet.getLong("INCREMENT_BY"));
            sequence.setCache(resultSet.getInt("CACHE_VALUE"));
            sequence.setCycle("T".equalsIgnoreCase(resultSet.getString("IS_CYCLED")));
            column.setSequence(sequence);
        }
    }
}
