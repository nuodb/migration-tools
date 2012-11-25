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
package com.nuodb.migration.jdbc.resolve;

import com.google.common.collect.Maps;
import com.nuodb.migration.utils.Reflections;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class DatabaseObjectResolverSupport<T> implements DatabaseObjectResolver<T> {

    private final transient Logger logger = LoggerFactory.getLogger(getClass());

    private Map<DatabaseInfoMatcher, Class<? extends T>> databaseInfoMatchers = Maps.newHashMap();

    private Class<? extends T> targetObjectClass;
    private Class<? extends T> defaultObjectClass;

    public DatabaseObjectResolverSupport(Class<? extends T> targetObjectClass, Class<? extends T> defaultObjectClass) {
        this.targetObjectClass = targetObjectClass;
        this.defaultObjectClass = defaultObjectClass;
    }

    public void register(String productName, Class<? extends T> objectClass) {
        register(new DatabaseInfoMatcherImpl(productName), objectClass);
    }

    public void register(String productName, String productVersion, Class<? extends T> objectClass) {
        register(new DatabaseInfoMatcherImpl(productName, productVersion), objectClass);
    }

    public void register(String productName, String productVersion, int majorVersion, Class<? extends T> objectClass) {
        register(new DatabaseInfoMatcherImpl(productName, productVersion, majorVersion), objectClass);
    }

    public void register(String productName, String productVersion, int majorVersion, int minorVersion,
                         Class<? extends T> objectClass) {
        register(new DatabaseInfoMatcherImpl(productName, productVersion, majorVersion, minorVersion), objectClass);
    }

    public void register(DatabaseInfoMatcher databaseInfoMatcher, Class<? extends T> objectClass) {
        databaseInfoMatchers.put(databaseInfoMatcher, objectClass);
    }

    @Override
    public T resolve(DatabaseMetaData metaData) throws SQLException {
        String productName = metaData.getDatabaseProductName();
        String productVersion = metaData.getDatabaseProductVersion();
        int minorVersion = metaData.getDatabaseMinorVersion();
        int majorVersion = metaData.getDatabaseMajorVersion();
        Class<? extends T> objectClass = null;
        for (Map.Entry<DatabaseInfoMatcher, Class<? extends T>> databaseInfoMatcherEntry : databaseInfoMatchers.entrySet()) {
            DatabaseInfoMatcher databaseInfoMatcher = databaseInfoMatcherEntry.getKey();
            if (databaseInfoMatcher.matches(productName, productVersion, minorVersion, majorVersion)) {
                objectClass = databaseInfoMatcherEntry.getValue();
                break;
            }
        }
        if (objectClass != null) {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Resolved %s to %s", targetObjectClass.getName(), objectClass.getName()));
            }
        } else if (defaultObjectClass != null) {
            if (logger.isWarnEnabled()) {
                logger.warn(format("Defaulted %s to %s", targetObjectClass.getName(), defaultObjectClass.getName()));
            }
            objectClass = defaultObjectClass;
        }
        return objectClass != null ? createObject(objectClass, metaData) : null;
    }

    protected T createObject(Class<? extends T> objectClass, DatabaseMetaData metaData) {
        return Reflections.newInstance(objectClass);
    }

    static class DatabaseInfoMatcherImpl implements DatabaseInfoMatcher {

        protected final String productName;
        protected final String productVersion;
        protected final Integer majorVersion;
        protected final Integer minorVersion;

        public DatabaseInfoMatcherImpl(String productName) {
            this(productName, null);
        }

        public DatabaseInfoMatcherImpl(String productName, String productVersion) {
            this(productName, productVersion, null);
        }

        public DatabaseInfoMatcherImpl(String productName, String productVersion, Integer majorVersion) {
            this(productName, productVersion, majorVersion, null);
        }

        public DatabaseInfoMatcherImpl(String productName, String productVersion, Integer majorVersion,
                                       Integer minorVersion) {
            this.productName = productName;
            this.productVersion = productVersion;
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
        }

        @Override
        public boolean matches(String productName, String productVersion, int majorVersion, int minorVersion) {
            if (matchesProductName(productName)) {
                return false;
            }
            if (matchesProductVersion(productVersion)) {
                return false;
            }
            if (matchesMajorVersion(majorVersion)) {
                return false;
            }
            if (matchesMinorVersion(minorVersion)) {
                return false;
            }
            return true;
        }

        protected boolean matchesProductName(String productName) {
            return !StringUtils.startsWithIgnoreCase(this.productName, productName);
        }

        protected boolean matchesProductVersion(String productVersion) {
            if (this.productVersion != null) {
                if (!StringUtils.equals(this.productVersion, productVersion)) {
                    return true;
                }
            }
            return false;
        }

        protected boolean matchesMajorVersion(int majorVersion) {
            if (this.majorVersion != null) {
                if (!this.majorVersion.equals(majorVersion)) {
                    return true;
                }
            }
            return false;
        }

        protected boolean matchesMinorVersion(int minorVersion) {
            if (this.minorVersion != null) {
                if (!this.minorVersion.equals(minorVersion)) {
                    return true;
                }
            }
            return false;
        }
    }
}