/**
 * Copyright (c) 2014, NuoDB, Inc.
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
package com.nuodb.migrator.backup.format.sql;

import com.nuodb.migrator.backup.format.OutputFormatBase;
import com.nuodb.migrator.backup.format.OutputFormatException;
import com.nuodb.migrator.backup.format.value.Value;
import com.nuodb.migrator.backup.format.value.ValueHandle;
import com.nuodb.migrator.backup.format.value.ValueHandleList;
import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.query.InsertQuery;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import static com.nuodb.migrator.context.ContextUtils.createService;
import static com.nuodb.migrator.jdbc.metadata.DatabaseInfos.NUODB;
import static java.lang.System.getProperty;
import static java.sql.Types.*;

/**
 * @author Sergey Bushik
 */
public class SqlOutputFormat extends OutputFormatBase implements SqlAttributes {

    private static final String SEMICOLON = ";";

    private String lineEnding = SEMICOLON;
    private String lineSeparator = getProperty("line.separator");
    private Dialect dialect;
    private Writer output;

    @Override
    public String getFormat() {
        return FORMAT;
    }

    @Override
    public void init() {
        super.init();
        dialect = initDialect();
    }

    protected Dialect initDialect() {
        return createService(DialectResolver.class).resolve(NUODB);
    }

    @Override
    protected void init(OutputStream outputStream) {
        init(new OutputStreamWriter(outputStream));
    }

    @Override
    protected void init(Writer writer) {
        output = wrapWriter(writer);
    }

    @Override
    public void writeStart() {
    }

    @Override
    public void writeValues(Value[] values) {
        InsertQuery insertQuery = new InsertQuery();
        insertQuery.setQualifyNames(false);
        insertQuery.setDialect(dialect);
        ValueHandleList valueHandleList = getValueHandleList();
        for (int index = 0; index < values.length; index++) {
            Value value = values[index];
            ValueHandle valueHandle = valueHandleList.get(index);
            Column column = getColumn(valueHandle);
            if (index == 0) {
                insertQuery.setInto(column.getTable());
            }
            insertQuery.addColumn(column, asString(value, valueHandle));
        }
        try {
            String script = insertQuery.toString();
            output.write(script);
            if (!script.endsWith(lineEnding)) {
                output.write(lineEnding);
            }
            output.write(lineSeparator);
        } catch (IOException exception) {
            throw new OutputFormatException(exception);
        }
    }

    protected Column getColumn(ValueHandle valueHandle) {
        return (Column) valueHandle.asField();
    }

    protected String asString(Value value, ValueHandle valueHandle) {
        String string;
        if (!value.isNull()) {
            switch (valueHandle.getTypeCode()) {
                case SMALLINT:
                case TINYINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case REAL:
                case DOUBLE:
                case NUMERIC:
                case DECIMAL:
                    string = value.asString();
                    break;
                default:
                    string = "'" + value.asString() + "'";
            }
        } else {
            string = "NULL";
        }
        return string;
    }

    @Override
    public boolean canWrite() {
        return true;
    }

    @Override
    public void writeEnd() {
    }

    public String getLineEnding() {
        return lineEnding;
    }

    public void setLineEnding(String lineEnding) {
        this.lineEnding = lineEnding;
    }

    public String getLineSeparator() {
        return lineSeparator;
    }

    public void setLineSeparator(String lineSeparator) {
        this.lineSeparator = lineSeparator;
    }

    public Dialect getDialect() {
        return dialect;
    }

    public void setDialect(Dialect dialect) {
        this.dialect = dialect;
    }

    @Override
    public void close() {
        if (output != null) {
            try {
                output.close();
            } catch (IOException exception) {
                throw new OutputFormatException(exception);
            }
            output = null;
        }
    }
}
