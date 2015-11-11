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
package com.nuodb.migrator.backup.format;

import com.nuodb.migrator.backup.Column;
import com.nuodb.migrator.backup.format.value.ValueType;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.Reader;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.io.Closeables.closeQuietly;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class InputBase extends FormatBase implements Input {

    private Reader reader;
    private InputStream inputStream;
    private List<ValueType> valueTypes;

    public Reader getReader() {
        return reader;
    }

    @Override
    public void setReader(Reader reader) {
        this.reader = reader;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    @Override
    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    public void init() {
        if (hasReader()) {
            init(openReader());
        } else if (hasInputStream()) {
            init(openInputStream());
        } else {
            throw new InputException("Reader or stream is required to read backup");
        }
        initValueTypes();
    }

    protected abstract void init(Reader reader);

    protected abstract void init(InputStream inputStream);

    public List<ValueType> getValueTypes() {
        return valueTypes;
    }

    protected void initValueTypes() {
        List<ValueType> valueTypes = newArrayList();
        for (Column column : getRowSet().getColumns()) {
            valueTypes.add(column.getValueType());
        }
        this.valueTypes = valueTypes;
    }

    protected boolean hasReader() {
        return reader != null;
    }

    protected Reader openReader() {
        return wrapReader(reader);
    }

    protected Reader wrapReader(Reader reader) {
        return isBuffering() ? new BufferedReader(reader, getBufferSize()) : reader;
    }

    protected boolean hasInputStream() {
        return inputStream != null;
    }

    protected InputStream openInputStream() {
        return wrapInputStream(inputStream);
    }

    protected InputStream wrapInputStream(InputStream inputStream) {
        return isBuffering() ? new BufferedInputStream(inputStream, getBufferSize()) : inputStream;
    }

    @Override
    public void close() {
        if (hasReader()) {
            close(getReader());
        } else if (hasInputStream()) {
            close(getInputStream());
        }
    }

    protected void close(Reader reader) {
        closeQuietly(reader);
    }

    protected void close(InputStream inputStream) {
        closeQuietly(inputStream);
    }
}
