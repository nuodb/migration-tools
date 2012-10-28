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
package com.nuodb.tools.migration.format.catalog;

import com.nuodb.tools.migration.dump.DumpException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.commons.io.FileUtils.openInputStream;

/**
 * @author Sergey Bushik
 */
public class QueryEntryReaderImpl implements QueryEntryReader {

    protected final Log log = LogFactory.getLog(getClass());

    private QueryEntryCatalogImpl catalog;
    private InputStream catalogInput;

    public QueryEntryReaderImpl(QueryEntryCatalogImpl catalog) {
        this.catalog = catalog;
    }

    protected void open() throws QueryEntryCatalogException {
        File catalogFile = catalog.getCatalogFile();
        if (log.isDebugEnabled()) {
            log.debug(String.format("Dump catalog file is %1$s", catalogFile.getPath()));
        }
        try {
            this.catalogInput = openInputStream(catalogFile);
        } catch (IOException e) {
            throw new DumpException("Error opening catalog file for writing", e);
        }
    }

    public QueryEntry read() throws QueryEntryCatalogException {
        return null;
    }

    @Override
    public void close() throws QueryEntryCatalogException {
        IOUtils.closeQuietly(catalogInput);
    }
}
