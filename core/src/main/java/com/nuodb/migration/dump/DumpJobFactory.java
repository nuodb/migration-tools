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
package com.nuodb.migration.dump;

import com.nuodb.migration.jdbc.connection.ConnectionProviderFactory;
import com.nuodb.migration.jdbc.dialect.DialectResolver;
import com.nuodb.migration.jdbc.dialect.SimpleDialectResolver;
import com.nuodb.migration.job.JobFactory;
import com.nuodb.migration.resultset.catalog.Catalog;
import com.nuodb.migration.resultset.catalog.FileCatalog;
import com.nuodb.migration.resultset.format.DefaultResultSetFormatFactory;
import com.nuodb.migration.resultset.format.ResultSetFormatFactory;
import com.nuodb.migration.resultset.format.jdbc.JdbcTypeValueFormatRegistryResolver;
import com.nuodb.migration.resultset.format.jdbc.SimpleJdbcTypeValueFormatRegistryResolver;
import com.nuodb.migration.spec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static com.nuodb.migration.utils.ValidationUtils.isNotNull;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class DumpJobFactory extends ConnectionProviderFactory implements JobFactory<DumpJob> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private DumpSpec dumpSpec;

    private DialectResolver dialectResolver =
            new SimpleDialectResolver();
    private ResultSetFormatFactory resultSetFormatFactory =
            new DefaultResultSetFormatFactory();
    private JdbcTypeValueFormatRegistryResolver jdbcTypeValueFormatRegistryResolver =
            new SimpleJdbcTypeValueFormatRegistryResolver();

    public DumpJob createJob() {
        isNotNull(dumpSpec, "Dump spec is required");
        ConnectionSpec connectionSpec = dumpSpec.getSourceSpec();
        Collection<SelectQuerySpec> selectQuerySpecs = dumpSpec.getSelectQuerySpecs();
        Collection<NativeQuerySpec> nativeQuerySpecs = dumpSpec.getNativeQuerySpecs();

        ResourceSpec outputSpec = dumpSpec.getOutputSpec();
        DumpJob job = new DumpJob();
        job.setConnectionProvider(createConnectionProvider(connectionSpec, false));
        job.setTimeZone(dumpSpec.getTimeZone());
        job.setSelectQuerySpecs(selectQuerySpecs);
        job.setNativeQuerySpecs(nativeQuerySpecs);
        job.setOutputType(outputSpec.getType());
        job.setAttributes(outputSpec.getAttributes());
        job.setCatalog(createCatalog(outputSpec.getPath()));
        job.setDialectResolver(dialectResolver);
        job.setResultSetFormatFactory(resultSetFormatFactory);
        job.setJdbcTypeValueFormatRegistryResolver(jdbcTypeValueFormatRegistryResolver);
        return job;
    }

    protected Catalog createCatalog(String path) {
        return new FileCatalog(path);
    }

    public DumpSpec getDumpSpec() {
        return dumpSpec;
    }

    public void setDumpSpec(DumpSpec dumpSpec) {
        this.dumpSpec = dumpSpec;
    }

    public DialectResolver getDialectResolver() {
        return dialectResolver;
    }

    public void setDialectResolver(DialectResolver dialectResolver) {
        this.dialectResolver = dialectResolver;
    }

    public ResultSetFormatFactory getResultSetFormatFactory() {
        return resultSetFormatFactory;
    }

    public void setResultSetFormatFactory(ResultSetFormatFactory resultSetFormatFactory) {
        this.resultSetFormatFactory = resultSetFormatFactory;
    }

    public JdbcTypeValueFormatRegistryResolver getJdbcTypeValueFormatRegistryResolver() {
        return jdbcTypeValueFormatRegistryResolver;
    }

    public void setJdbcTypeValueFormatRegistryResolver(
            JdbcTypeValueFormatRegistryResolver jdbcTypeValueFormatRegistryResolver) {
        this.jdbcTypeValueFormatRegistryResolver = jdbcTypeValueFormatRegistryResolver;
    }
}