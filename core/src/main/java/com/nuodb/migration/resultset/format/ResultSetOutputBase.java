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
package com.nuodb.migration.resultset.format;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.nuodb.migration.jdbc.model.ValueModel;
import com.nuodb.migration.jdbc.model.ValueModelList;
import com.nuodb.migration.jdbc.type.JdbcTypeDesc;
import com.nuodb.migration.jdbc.type.access.JdbcTypeValueAccess;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.nuodb.migration.jdbc.model.ValueModelFactory.createValueModelList;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class ResultSetOutputBase extends ResultSetFormatBase implements ResultSetOutput {

    private transient final Logger logger = LoggerFactory.getLogger(getClass());

    private Writer writer;
    private OutputStream outputStream;
    private ResultSet resultSet;

    public Writer getWriter() {
        return writer;
    }

    @Override
    public void setWriter(Writer writer) {
        this.writer = writer;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    @Override
    public void setOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void initOutputModel() {
        ValueModelList<ValueModel> valueModelList = getValueModelList();
        if (valueModelList == null) {
            try {
                setValueModelList(valueModelList = createValueModelList(resultSet));
            } catch (SQLException exception) {
                throw new ResultSetOutputException(exception);
            }
        }

        ValueModelList<ResultSetValueModel> resultSetValueModelList = getResultSetValueModelList();
        if (resultSetValueModelList == null) {
            resultSetValueModelList = createValueModelList(Iterables.transform(valueModelList,
                    new Function<ValueModel, ResultSetValueModel>() {

                        private int index = 0;

                        @Override
                        public ResultSetValueModel apply(ValueModel valueModel) {
                            return visitValueFormatModel(createValueFormatModel(valueModel, index++));
                        }
                    }));
            setResultSetValueModelList(resultSetValueModelList);
        }
    }

    @Override
    public final void writeBegin() {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Write begin %s", getClass().getName()));
        }
        doWriteBegin();
    }

    protected ResultSetValueModel createValueFormatModel(ValueModel valueModel, int index) {
        JdbcTypeDesc jdbcTypeDesc = new JdbcTypeDesc(valueModel.getTypeCode(), valueModel.getTypeName());
        JdbcTypeDesc jdbcTypeDescAlias = getJdbcTypeValueAccessProvider().getJdbcTypeDescAlias(jdbcTypeDesc);

        valueModel.setTypeCode(jdbcTypeDescAlias.getTypeCode());
        valueModel.setTypeName(jdbcTypeDescAlias.getTypeName());

        JdbcTypeValueFormat jdbcTypeValueFormat =
                getJdbcTypeValueFormatRegistry().getJdbcTypeValueFormat(jdbcTypeDescAlias);
        JdbcTypeValueAccess<Object> jdbcTypeValueAccess =
                getJdbcTypeValueAccessProvider().getResultSetAccess(getResultSet(), valueModel, index + 1);
        return new SimpleResultSetValueModel(valueModel, jdbcTypeValueFormat, jdbcTypeValueAccess, null);
    }

    protected abstract void doWriteBegin();

    @Override
    public final void writeRow() {
        writeRow(getValues());
    }

    protected String[] getValues() {
        int index = 0;
        final ValueModelList<ResultSetValueModel> resultSetValueModelList = getResultSetValueModelList();
        final String[] values = new String[resultSetValueModelList.size()];
        for (ResultSetValueModel resultSetValueModel : resultSetValueModelList) {
            values[index++] = resultSetValueModel.getJdbcTypeValueFormat().getValue(
                    resultSetValueModel.getJdbcTypeValueAccess(), resultSetValueModel.getJdbcTypeValueAccessOptions());
        }
        return values;
    }

    protected abstract void writeRow(String[] values);

    @Override
    public void writeEnd() {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("End result output %s", getClass().getName()));
        }
        doWriteEnd();
    }

    protected abstract void doWriteEnd();

    public ResultSet getResultSet() {
        return resultSet;
    }

    @Override
    public void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }
}
