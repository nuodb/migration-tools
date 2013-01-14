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

import org.apache.commons.lang3.StringUtils;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * @author Sergey Bushik
 */
public class DatabaseInfo {

    private String productName;
    private String productVersion;
    private Integer majorVersion;
    private Integer minorVersion;

    public DatabaseInfo(String productName) {
        this(productName, null);
    }

    public DatabaseInfo(String productName, String productVersion) {
        this(productName, productVersion, null);
    }

    public DatabaseInfo(String productName, String productVersion, Integer majorVersion) {
        this(productName, productVersion, majorVersion, null);
    }

    public DatabaseInfo(DatabaseMetaData databaseMetaData) throws SQLException {
        this(databaseMetaData.getDatabaseProductName(), databaseMetaData.getDatabaseProductVersion(),
                databaseMetaData.getDatabaseMinorVersion(), databaseMetaData.getDatabaseMajorVersion());
    }

    public DatabaseInfo(String productName, String productVersion, Integer majorVersion, Integer minorVersion) {
        this.productName = productName;
        this.productVersion = productVersion;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
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

    public boolean matches(DatabaseInfo databaseInfo) {
        if (productName != null ? !StringUtils.startsWithIgnoreCase(productName,
                databaseInfo.productName) : databaseInfo.productName != null) {
            return false;
        }
        if (productVersion != null && databaseInfo.productVersion != null &&
                !StringUtils.equals(productVersion, databaseInfo.productVersion)) {
            return false;
        }
        if (majorVersion != null && databaseInfo.majorVersion != null &&
                majorVersion.equals(databaseInfo.majorVersion)) {
            return false;
        }
        if (minorVersion != null && databaseInfo.minorVersion != null &&
                minorVersion.equals(databaseInfo.minorVersion)) {
            return false;
        }
        return true;

    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DatabaseInfo that = (DatabaseInfo) o;

        if (productName != null ? !StringUtils.startsWithIgnoreCase(productName,
                that.productName) : that.productName != null) return false;
        if (productVersion != null ? !StringUtils.equals(productVersion,
                that.productVersion) : that.productVersion != null) return false;
        if (majorVersion != null ? !majorVersion.equals(that.majorVersion) : that.majorVersion != null) return false;
        if (minorVersion != null ? !minorVersion.equals(that.minorVersion) : that.minorVersion != null) return false;
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
}