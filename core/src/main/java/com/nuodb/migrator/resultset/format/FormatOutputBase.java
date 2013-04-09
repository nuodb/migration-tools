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
package com.nuodb.migrator.resultset.format;

import com.nuodb.migrator.jdbc.model.ValueModel;
import com.nuodb.migrator.jdbc.model.ValueModelList;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;
import com.nuodb.migrator.jdbc.type.access.JdbcTypeValueAccess;
import com.nuodb.migrator.resultset.format.value.ValueFormat;
import com.nuodb.migrator.resultset.format.value.ValueFormatModel;
import com.nuodb.migrator.resultset.format.value.ValueVariant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.nuodb.migrator.jdbc.model.ValueModelFactory.createValueModelList;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class FormatOutputBase extends FormatBase implements FormatOutput {

    public static final boolean DEFAULT_BUFFERING = true;
    public static final int DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024;

    private transient final Logger logger = LoggerFactory.getLogger(getClass());

    private Writer writer;
    private OutputStream outputStream;
    private ResultSet resultSet;
    private int row;
    private boolean buffering = DEFAULT_BUFFERING;
    private int bufferSize = DEFAULT_BUFFER_SIZE;

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

    protected Writer wrapWriter(Writer writer) {
        return isBuffering() ? new BufferedWriter(writer, getBufferSize()) : writer;
    }

    protected OutputStream wrapOutputStream(OutputStream outputStream) {
        return isBuffering() ? new BufferedOutputStream(outputStream, getBufferSize()) : outputStream;
    }

    @Override
    public void initValueFormatModel() throws SQLException {
        ValueModelList<ValueModel> valueModelList = createValueModelList(resultSet.getMetaData());
        int index = 0;
        for (ValueFormatModel valueFormatModel : getValueFormatModelList()) {
            ValueModel valueModel = valueModelList.get(index);
            JdbcTypeDesc typeDescAlias = getValueAccessProvider().getJdbcTypeDescAlias(
                    new JdbcTypeDesc(valueModel.getTypeCode(), valueModel.getTypeName())
            );
            valueFormatModel.setTypeCode(typeDescAlias.getTypeCode());
            valueFormatModel.setTypeName(typeDescAlias.getTypeName());

            ValueFormat valueFormat = getValueFormatRegistry().getValueFormat(typeDescAlias);
            JdbcTypeValueAccess valueAccess = getValueAccessProvider().getResultSetAccess(
                    getResultSet(), valueFormatModel, index + 1);
            valueFormatModel.setValueVariantType(valueFormat.getVariantType(valueFormatModel));
            valueFormatModel.setValueFormat(valueFormat);
            valueFormatModel.setValueAccess(valueAccess);
            visitValueFormatModel(valueFormatModel);
            index++;
        }
    }

    @Override
    public boolean hasNextRow() throws SQLException {
        return resultSet != null && resultSet.next();
    }

    @Override
    public final void writeBegin() {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Write begin %s", getClass().getName()));
        }
        doWriteBegin();
    }

    protected abstract void doWriteBegin();

    @Override
    public final void writeRow() {
        ValueVariant[] values = null;
        try {
            writeValues(values = getValues());
            row++;
        } catch (Exception exception) {
            onWriteRowFailure(exception, row, values);
        }
    }

    protected ValueVariant[] getValues() {
        int index = 0;
        ValueModelList<ValueFormatModel> valueFormatModelList = getValueFormatModelList();
        ValueVariant[] values = new ValueVariant[valueFormatModelList.size()];
        for (ValueFormatModel valueFormatModel : valueFormatModelList) {
            values[index++] = valueFormatModel.getValueFormat().getValue(
                    valueFormatModel.getValueAccess(), valueFormatModel.getValueAccessOptions());

        }
        return values;
    }

    protected abstract void writeValues(ValueVariant[] variants);

    protected void onWriteRowFailure(Exception exception, int row, ValueVariant[] values) {
        String message = format("Failed to dump row %d", row);
        if (isLenient()) {
            if (logger.isErrorEnabled()) {
                logger.error(message);
            }
        } else {
            throw new FormatOutputException(message, exception);
        }
    }

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

    public boolean isBuffering() {
        return buffering;
    }

    public void setBuffering(boolean buffering) {
        this.buffering = buffering;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}
