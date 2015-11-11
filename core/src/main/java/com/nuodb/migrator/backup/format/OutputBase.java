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

import com.nuodb.migrator.utils.Counting;
import com.nuodb.migrator.utils.CountingOutputStream;
import com.nuodb.migrator.utils.CountingWriter;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.Writer;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public abstract class OutputBase extends FormatBase implements Output {

    private Writer writer;
    private OutputStream outputStream;
    private Long maxSize;
    private Counting counting;

    protected OutputBase() {
    }

    protected OutputBase(Long maxSize) {
        this.maxSize = maxSize;
    }

    @Override
    public Writer getWriter() {
        return writer;
    }

    @Override
    public void setWriter(Writer writer) {
        this.writer = writer;
    }

    @Override
    public OutputStream getOutputStream() {
        return outputStream;
    }

    @Override
    public void setOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void init() {
        if (hasWriter()) {
            init(openWriter());
        } else if (hasOutputStream()) {
            init(openOutputStream());
        } else {
            throw new OutputException("Writer or stream is required to export backup");
        }
    }

    protected abstract void init(Writer writer);

    protected abstract void init(OutputStream outputStream);

    protected boolean hasWriter() {
        return writer != null;
    }

    protected Writer openWriter() {
        return wrapWriter(writer);
    }

    protected Writer wrapWriter(Writer writer) {
        writer = isCounting() ? (Writer) (counting = new CountingWriter(writer)) : writer;
        writer = isBuffering() ? new BufferedWriter(writer, getBufferSize()) : writer;
        return writer;
    }

    protected boolean hasOutputStream() {
        return outputStream != null;
    }

    protected OutputStream openOutputStream() {
        return wrapOutputStream(outputStream);
    }

    protected OutputStream wrapOutputStream(OutputStream outputStream) {
        outputStream = isCounting() ? (OutputStream) (counting = new CountingOutputStream(outputStream)) : outputStream;
        outputStream = isBuffering() ? new BufferedOutputStream(outputStream, getBufferSize()) : outputStream;
        return outputStream;
    }

    @Override
    public boolean canWrite() {
        return fitMaxSize();
    }

    protected boolean fitMaxSize() {
        return !(getMaxSize() != null && counting != null) || counting.getCount() < getMaxSize();
    }

    public boolean isCounting() {
        return getMaxSize() != null;
    }

    public Long getMaxSize() {
        return maxSize;
    }
}
