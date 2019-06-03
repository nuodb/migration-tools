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
package com.nuodb.migrator.jdbc;

import com.nuodb.migrator.jdbc.connection.ConnectionProxy;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptProcessor;
import com.nuodb.migrator.jdbc.session.Session;
import com.nuodb.migrator.jdbc.url.JdbcUrl;
import com.nuodb.migrator.spec.DriverConnectionSpec;

import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public class JdbcUtils {

    private static transient final Logger logger = getLogger(JdbcUtils.class);

    private JdbcUtils() {
    }

    public static JdbcUrl getJdbcUrl(Connection connection) {
        return ((ConnectionProxy<DriverConnectionSpec>) connection).getConnectionSpec().getJdbcUrl();
    }

    public static void closeQuietly(ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException exception) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed closing result set", exception);
            }
        }
    }

    public static void closeQuietly(Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException exception) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed closing statement", exception);
            }
        }
    }

    public static void closeQuietly(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException exception) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed closing connection", exception);
            }
        }
    }

    public static void closeQuietly(Session session) {
        try {
            if (session != null) {
                session.close();
            }
        } catch (SQLException exception) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed closing session", exception);
            }
        }
    }

    public static void closeQuietly(ScriptProcessor scriptProcessor) {
        try {
            if (scriptProcessor != null) {
                scriptProcessor.close();
            }
        } catch (Exception exception) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed closing script processor", exception);
            }
        }
    }
}
