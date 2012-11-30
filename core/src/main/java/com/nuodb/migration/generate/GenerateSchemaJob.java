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
package com.nuodb.migration.generate;

import com.nuodb.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.migration.jdbc.connection.ConnectionProxy;
import com.nuodb.migration.jdbc.connection.ConnectionServices;
import com.nuodb.migration.jdbc.metadata.Database;
import com.nuodb.migration.job.JobBase;
import com.nuodb.migration.job.JobExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

import static com.nuodb.migration.jdbc.JdbcUtils.close;
import static com.nuodb.migration.utils.ValidationUtils.isNotNull;

/**
 * @author Sergey Bushik
 */
public class GenerateSchemaJob extends JobBase {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private ConnectionProvider sourceConnectionProvider;
    private ConnectionProvider targetConnectionProvider;

    @Override
    public void execute(JobExecution execution) throws Exception {
        validate();
        execution(new GenerateSchemaJobExecution(execution));
    }

    protected void validate() {
        isNotNull(getSourceConnectionProvider(), "Source connection provider is required");
        isNotNull(getTargetConnectionProvider(), "Target connection provider is required");
    }

    protected void execution(GenerateSchemaJobExecution execution) throws SQLException {
        ConnectionServices sourceConnectionServices = getSourceConnectionProvider().getConnectionServices();
        ConnectionServices targetConnectionServices = getTargetConnectionProvider().getConnectionServices();
        try {
            execution.setSourceConnectionServices(sourceConnectionServices);
            execution.setTargetConnectionServices(targetConnectionServices);
            generate(execution);
        } finally {
            close(sourceConnectionServices);
            close(targetConnectionServices);
        }
    }

    protected void generate(GenerateSchemaJobExecution execution) throws SQLException {
        ConnectionServices sourceConnectionServices = execution.getSourceConnectionServices();
        Database sourceDatabase = sourceConnectionServices.createDatabaseInspector().inspect();
        Connection connection = sourceConnectionServices.getConnection();
        if (connection instanceof ConnectionProxy) {
            Connection connection1 = ((ConnectionProxy) connection).getConnection();
            System.out.println(connection1);
        }
        if (logger.isInfoEnabled()) {
            logger.info("Source database inspected");
        }
        System.out.println(sourceDatabase);
        ConnectionServices targetConnectionServices = execution.getTargetConnectionServices();
        Database targetDatabase = targetConnectionServices.createDatabaseInspector().inspect();
        if (logger.isInfoEnabled()) {
            logger.info("Target database inspected");
        }
        System.out.println(targetDatabase);
    }

    public ConnectionProvider getSourceConnectionProvider() {
        return sourceConnectionProvider;
    }

    public void setSourceConnectionProvider(ConnectionProvider sourceConnectionProvider) {
        this.sourceConnectionProvider = sourceConnectionProvider;
    }

    public ConnectionProvider getTargetConnectionProvider() {
        return targetConnectionProvider;
    }

    public void setTargetConnectionProvider(ConnectionProvider targetConnectionProvider) {
        this.targetConnectionProvider = targetConnectionProvider;
    }
}
