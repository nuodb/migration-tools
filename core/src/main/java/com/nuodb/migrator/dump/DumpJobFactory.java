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
package com.nuodb.migrator.dump;

import com.nuodb.migrator.jdbc.connection.ConnectionProviderFactory;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.dialect.SimpleDialectResolver;
import com.nuodb.migrator.job.JobFactory;
import com.nuodb.migrator.resultset.catalog.Catalog;
import com.nuodb.migrator.resultset.catalog.FileCatalog;
import com.nuodb.migrator.resultset.format.FormatFactory;
import com.nuodb.migrator.resultset.format.SimpleFormatFactory;
import com.nuodb.migrator.resultset.format.value.SimpleValueFormatRegistryResolver;
import com.nuodb.migrator.resultset.format.value.ValueFormatRegistryResolver;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.spec.DumpSpec;
import com.nuodb.migrator.spec.ResourceSpec;

import static com.nuodb.migrator.utils.ValidationUtils.isNotNull;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class DumpJobFactory extends ConnectionProviderFactory implements JobFactory<DumpJob> {

    private DumpSpec dumpSpec;

    private DialectResolver dialectResolver = new SimpleDialectResolver();
    private FormatFactory formatFactory = new SimpleFormatFactory();
    private ValueFormatRegistryResolver valueFormatRegistryResolver = new SimpleValueFormatRegistryResolver();

    public DumpJob createJob() {
        isNotNull(dumpSpec, "Dump spec is required");

        DumpJob dumpJob = new DumpJob();
        ConnectionSpec connectionSpec = dumpSpec.getConnectionSpec();
        dumpJob.setConnectionProvider(createConnectionProvider(connectionSpec));
        ResourceSpec outputSpec = dumpSpec.getOutputSpec();
        dumpJob.setOutputType(outputSpec.getType());
        dumpJob.setOutputAttributes(outputSpec.getAttributes());
        dumpJob.setCatalog(createCatalog(outputSpec.getPath()));
        dumpJob.setTimeZone(dumpSpec.getTimeZone());
        dumpJob.setSelectQuerySpecs(dumpSpec.getSelectQuerySpecs());
        dumpJob.setNativeQuerySpecs(dumpSpec.getNativeQuerySpecs());
        dumpJob.setTableTypes(dumpSpec.getTableTypes());
        dumpJob.setDialectResolver(getDialectResolver());
        dumpJob.setFormatFactory(getFormatFactory());
        dumpJob.setValueFormatRegistryResolver(getValueFormatRegistryResolver());
        return dumpJob;
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

    public FormatFactory getFormatFactory() {
        return formatFactory;
    }

    public void setFormatFactory(FormatFactory formatFactory) {
        this.formatFactory = formatFactory;
    }

    public ValueFormatRegistryResolver getValueFormatRegistryResolver() {
        return valueFormatRegistryResolver;
    }

    public void setValueFormatRegistryResolver(
            ValueFormatRegistryResolver valueFormatRegistryResolver) {
        this.valueFormatRegistryResolver = valueFormatRegistryResolver;
    }
}