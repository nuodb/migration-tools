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
package com.nuodb.migrator;

import com.nuodb.migrator.backup.format.FormatFactory;
import com.nuodb.migrator.backup.format.value.ValueFormatRegistryResolver;
import com.nuodb.migrator.dump.DumpJob;
import com.nuodb.migrator.jdbc.connection.ConnectionProviderFactory;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionManager;
import com.nuodb.migrator.job.Job;
import com.nuodb.migrator.job.JobExecutor;
import com.nuodb.migrator.job.TraceJobExecutionListener;
import com.nuodb.migrator.load.LoadJob;
import com.nuodb.migrator.schema.SchemaJob;
import com.nuodb.migrator.spec.DumpJobSpec;
import com.nuodb.migrator.spec.LoadJobSpec;
import com.nuodb.migrator.spec.SchemaJobSpec;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import static com.nuodb.migrator.job.JobExecutors.createJobExecutor;
import static com.nuodb.migrator.utils.ReflectionUtils.getClassLoader;
import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public class Migrator {

    private static final transient Logger logger = getLogger(Migrator.class);

    private static final String PROPERTIES = "com/nuodb/migrator/migrator.properties";
    private static final String VERSION_PROPERTY = "com.nuodb.migrator.version";
    private static String VERSION;

    private FormatFactory formatFactory;
    private DialectResolver dialectResolver;
    private InspectionManager inspectionManager;
    private ConnectionProviderFactory connectionProviderFactory;
    private ValueFormatRegistryResolver valueFormatRegistryResolver;

    static {
        Properties properties = new Properties();
        try {
            InputStream input = getClassLoader().getResourceAsStream(PROPERTIES);
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException exception) {
            if (logger.isWarnEnabled()) {
                logger.warn(format("Can't read %s", PROPERTIES), exception);
            }
        }
        VERSION = properties.getProperty(VERSION_PROPERTY);
    }

    public static String getVersion() {
        return VERSION;
    }

    public void execute(DumpJobSpec jobSpec, Map<Object, Object> context) {
        execute(new DumpJob(jobSpec), context);
    }

    public void execute(LoadJobSpec jobSpec, Map<Object, Object> context) {
        execute(new LoadJob(jobSpec), context);
    }

    public void execute(SchemaJobSpec jobSpec, Map<Object, Object> context) {
        execute(new SchemaJob(jobSpec), context);
    }

    public void execute(Job job, Map<Object, Object> context) {
        JobExecutor jobExecutor = createJobExecutor(job);
        jobExecutor.addJobExecutionListener(new TraceJobExecutionListener());
        jobExecutor.execute(context);
        Throwable failure = jobExecutor.getJobStatus().getFailure();
        if (failure != null) {
            if (failure instanceof MigratorException) {
                throw (MigratorException)failure;
            } else {
                throw new MigratorException(failure);
            }
        }
    }

    public FormatFactory getFormatFactory() {
        return formatFactory;
    }

    public void setFormatFactory(FormatFactory formatFactory) {
        this.formatFactory = formatFactory;
    }

    public DialectResolver getDialectResolver() {
        return dialectResolver;
    }

    public void setDialectResolver(DialectResolver dialectResolver) {
        this.dialectResolver = dialectResolver;
    }

    public InspectionManager getInspectionManager() {
        return inspectionManager;
    }

    public void setInspectionManager(InspectionManager inspectionManager) {
        this.inspectionManager = inspectionManager;
    }

    public ConnectionProviderFactory getConnectionProviderFactory() {
        return connectionProviderFactory;
    }

    public void setConnectionProviderFactory(ConnectionProviderFactory connectionProviderFactory) {
        this.connectionProviderFactory = connectionProviderFactory;
    }

    public ValueFormatRegistryResolver getValueFormatRegistryResolver() {
        return valueFormatRegistryResolver;
    }

    public void setValueFormatRegistryResolver(ValueFormatRegistryResolver valueFormatRegistryResolver) {
        this.valueFormatRegistryResolver = valueFormatRegistryResolver;
    }
}
