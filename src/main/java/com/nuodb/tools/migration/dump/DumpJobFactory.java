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
package com.nuodb.tools.migration.dump;

import com.nuodb.tools.migration.dump.catalog.QueryEntryCatalog;
import com.nuodb.tools.migration.dump.catalog.EntryCatalogImpl;
import com.nuodb.tools.migration.dump.output.OutputFormatFactory;
import com.nuodb.tools.migration.dump.output.OutputFormatFactoryImpl;
import com.nuodb.tools.migration.jdbc.JdbcServices;
import com.nuodb.tools.migration.jdbc.JdbcServicesImpl;
import com.nuodb.tools.migration.job.JobFactory;
import com.nuodb.tools.migration.spec.ConnectionSpec;
import com.nuodb.tools.migration.spec.DriverManagerConnectionSpec;
import com.nuodb.tools.migration.spec.DumpSpec;
import com.nuodb.tools.migration.spec.FormatSpec;
import com.nuodb.tools.migration.spec.NativeQuerySpec;
import com.nuodb.tools.migration.spec.SelectQuerySpec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class DumpJobFactory implements JobFactory<DumpJob> {

    protected final Log log = LogFactory.getLog(getClass());

    private DumpSpec dumpSpec;
    private OutputFormatFactory outputFormatFactory = new OutputFormatFactoryImpl();

    public DumpJob createJob() {
        ConnectionSpec connectionSpec = dumpSpec.getConnectionSpec();
        Collection<SelectQuerySpec> selectQuerySpecs = dumpSpec.getSelectQuerySpecs();
        Collection<NativeQuerySpec> nativeQuerySpecs = dumpSpec.getNativeQuerySpecs();
        FormatSpec outputSpec = dumpSpec.getOutputSpec();

        DumpJob job = new DumpJob();
        job.setJdbcServices(createJdbcServices(connectionSpec));
        job.setQueryEntryCatalog(createQueryEntryCatalog(outputSpec));
        job.setSelectQuerySpecs(selectQuerySpecs);
        job.setNativeQuerySpecs(nativeQuerySpecs);
        job.setOutputType(outputSpec.getType());
        job.setOutputAttributes(outputSpec.getAttributes());
        job.setOutputFormatFactory(outputFormatFactory);
        return job;
    }

    protected JdbcServices createJdbcServices(ConnectionSpec connectionSpec) {
        return new JdbcServicesImpl((DriverManagerConnectionSpec) connectionSpec);
    }

    protected QueryEntryCatalog createQueryEntryCatalog(FormatSpec outputSpec) {
        return new EntryCatalogImpl(outputSpec.getPath(), outputSpec.getType());
    }

    public DumpSpec getDumpSpec() {
        return dumpSpec;
    }

    public void setDumpSpec(DumpSpec dumpSpec) {
        this.dumpSpec = dumpSpec;
    }

    public OutputFormatFactory getOutputFormatFactory() {
        return outputFormatFactory;
    }

    public void setOutputFormatFactory(OutputFormatFactory outputFormatFactory) {
        this.outputFormatFactory = outputFormatFactory;
    }
}