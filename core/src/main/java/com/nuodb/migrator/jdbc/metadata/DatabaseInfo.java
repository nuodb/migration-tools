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
package com.nuodb.migrator.jdbc.metadata;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.nuodb.migrator.utils.ObjectUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.Serializable;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static com.google.common.collect.ComparisonChain.start;
import static com.google.common.collect.Ordering.natural;
import static org.apache.commons.lang3.StringUtils.startsWithIgnoreCase;
import static org.slf4j.LoggerFactory.getLogger;
import static java.lang.String.format;

public class DatabaseInfo implements Comparable<DatabaseInfo>, Serializable {

    private transient final Logger logger = getLogger(getClass());

    protected static final int ASSIGNABLE = 0;
    protected static final int NOT_ASSIGNABLE = 1;
    private static final int DRIVER_ERROR = 1;

    private String productName;
    private String productVersion;
    private Integer majorVersion;
    private Integer minorVersion;

    public DatabaseInfo() {
    }

    public DatabaseInfo(String productName) {
        this.productName = productName;
    }

    public DatabaseInfo(String productName, String productVersion) {
        this.productName = productName;
        this.productVersion = productVersion;
    }

    public DatabaseInfo(String productName, String productVersion, Integer majorVersion) {
        this.productName = productName;
        this.productVersion = productVersion;
        this.majorVersion = majorVersion;
    }

    public DatabaseInfo(String productName, String productVersion, Integer majorVersion, Integer minorVersion) {
        this.productName = productName;
        this.productVersion = productVersion;
        this.minorVersion = minorVersion;
        this.majorVersion = majorVersion;
    }

    public DatabaseInfo(DatabaseMetaData metaData) throws SQLException {
        try {
            this.productName = metaData.getDatabaseProductName();
            this.productVersion = metaData.getDatabaseProductVersion();
            this.minorVersion = metaData.getDatabaseMinorVersion();
            this.majorVersion = metaData.getDatabaseMajorVersion();
        } catch (Error e) {
            if (logger.isErrorEnabled()) {
                logger.error((format(
                        "JDBC Driver %s version is not applicable for Migrator, use latest version of the driver",
                        metaData.getDriverVersion())));
            }
            System.exit(DRIVER_ERROR);
        }
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductVersion() {
        return productVersion;
    }

    public void setProductVersion(String productVersion) {
        this.productVersion = productVersion;
    }

    public Integer getMajorVersion() {
        return majorVersion;
    }

    public void setMajorVersion(Integer majorVersion) {
        this.majorVersion = majorVersion;
    }

    public Integer getMinorVersion() {
        return minorVersion;
    }

    public void setMinorVersion(Integer minorVersion) {
        this.minorVersion = minorVersion;
    }

    /**
     * Checks if this is a base database info for the given database info
     *
     * @param databaseInfo
     *            to check if it's inherited from this database info
     * @return true is this database into is a super class for the given info
     */
    public boolean isAssignable(DatabaseInfo databaseInfo) {
        return isAssignable(databaseInfo, start()).result() <= 0;
    }

    protected ComparisonChain isAssignable(DatabaseInfo databaseInfo, ComparisonChain comparator) {
        comparator = isAssignableProductName(databaseInfo, comparator);
        comparator = isAssignableProductVersion(databaseInfo, comparator);
        comparator = isAssignableMajorVersion(databaseInfo, comparator);
        comparator = isAssignableMinorVersion(databaseInfo, comparator);
        return comparator;
    }

    protected ComparisonChain isAssignableProductName(DatabaseInfo databaseInfo, ComparisonChain comparator) {
        return comparator.compare(getProductName(), databaseInfo.getProductName(), new Ordering<String>() {
            @Override
            public int compare(String productName1, String productName2) {
                return productName1 == null ? 0
                        : StringUtils.equals(productName1, productName2) ? ASSIGNABLE : NOT_ASSIGNABLE;
            }
        });
    }

    protected ComparisonChain isAssignableProductVersion(DatabaseInfo databaseInfo, ComparisonChain comparator) {
        return comparator.compare(getProductVersion(), databaseInfo.getProductVersion(), new Ordering<String>() {
            @Override
            public int compare(String productVersion1, String productVersion2) {
                return productVersion1 == null ? 0
                        : StringUtils.equals(productVersion1, productVersion2) ? ASSIGNABLE : NOT_ASSIGNABLE;
            }
        });
    }

    protected ComparisonChain isAssignableMajorVersion(DatabaseInfo databaseInfo, ComparisonChain comparator) {
        return comparator.compare(getMajorVersion(), databaseInfo.getMajorVersion(), natural().nullsFirst());
    }

    protected ComparisonChain isAssignableMinorVersion(DatabaseInfo databaseInfo, ComparisonChain comparator) {
        return comparator.compare(getMinorVersion(), databaseInfo.getMinorVersion(), natural().nullsFirst());
    }

    @Override
    public int compareTo(DatabaseInfo databaseInfo) {
        return isAssignable(databaseInfo, start()).result();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DatabaseInfo that = (DatabaseInfo) o;

        if (productName != null ? !startsWithIgnoreCase(productName, that.productName) : that.productName != null)
            return false;
        if (productVersion != null ? !StringUtils.equals(productVersion, that.productVersion)
                : that.productVersion != null)
            return false;
        if (majorVersion != null ? !majorVersion.equals(that.majorVersion) : that.majorVersion != null)
            return false;
        if (minorVersion != null ? !minorVersion.equals(that.minorVersion) : that.minorVersion != null)
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = productName != null ? productName.hashCode() : 0;
        result = 31 * result + (productVersion != null ? productVersion.hashCode() : 0);
        result = 31 * result + (majorVersion != null ? majorVersion.hashCode() : 0);
        result = 31 * result + (minorVersion != null ? minorVersion.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this);
    }
}