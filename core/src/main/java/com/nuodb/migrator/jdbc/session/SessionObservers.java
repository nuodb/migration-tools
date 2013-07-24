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
package com.nuodb.migrator.jdbc.session;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.TimeZone;

import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Sergey Bushik
 */
public class SessionObservers {

    private static final String DEFAULT_TIME_ZONE = "default";

    /**
     * Sets transaction isolation level to one of the supported values from specified array.
     *
     * @param levels to use for setting transaction isolation.
     * @return session observer.
     */
    public static SessionObserver newTransactionIsolationObserver(final int[] levels) {
        return new SessionObserver() {
            @Override
            public void afterOpen(Session session) throws SQLException {
                session.getDialect().setTransactionIsolation(session.getConnection(), levels);
            }

            @Override
            public void beforeClose(Session session) throws SQLException {
            }
        };
    }

    /**
     * Sets session time zone of dialect supports its.
     *
     * @param timeZone to use in set statement.
     * @return session observer.
     */
    public static SessionObserver newSessionTimeZoneObserver(final TimeZone timeZone) {
        return new SessionObserver() {
            private final transient Logger logger = getLogger(getClass());

            @Override
            public void afterOpen(Session session) throws SQLException {
                setSessionTimeZone(session, timeZone);
            }

            @Override
            public void beforeClose(Session session) throws SQLException {
                setSessionTimeZone(session, null);
            }

            private void setSessionTimeZone(Session session, TimeZone timeZone) throws SQLException {
                Dialect dialect = session.getDialect();
                if (dialect.supportsSessionTimeZone()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(format("Session %s time zone set to %s", session, getTimeZoneName(timeZone)));
                    }
                    dialect.setSessionTimeZone(session.getConnection(), timeZone);
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.trace(format("Session time zone is not supported by %s", getDialectName(dialect)));
                    }
                }
            }

            private String getTimeZoneName(TimeZone timeZone) {
                return timeZone != null ? timeZone.getID() : DEFAULT_TIME_ZONE;
            }

            private Object getDialectName(Dialect dialect) {
                return dialect.getClass().getName();
            }
        };
    }
}
