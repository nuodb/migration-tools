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

import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

import com.nuodb.migrator.utils.ObjectUtils;

import java.io.Serializable;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.slf4j.Logger;

/**
 * @author Sergey Bushik
 */
public class DriverInfo implements Serializable {

    private transient final Logger logger = getLogger(getClass());

    private String name;
    private String version;
    private static final String MySQL = "mysql";
    private static final int DRIVER_ERROR = 1;
    private int majorVersion;
    private int minorVersion;

    public DriverInfo() {
    }

    public DriverInfo(String name, String version, int minorVersion, int majorVersion) {
        this.name = name;
        this.version = version;
        this.minorVersion = minorVersion;
        this.majorVersion = majorVersion;
    }

    public DriverInfo(DatabaseMetaData metaData) throws SQLException {
        this.name = metaData.getDriverName();
        this.version = metaData.getDriverVersion();
        this.majorVersion = metaData.getDriverMajorVersion();
        this.minorVersion = metaData.getDriverMinorVersion();
        validateDriverVersion(metaData);
    }

    private void validateDriverVersion(DatabaseMetaData metaData) {
        String url;
        try {
            url = metaData.getURL();
            if (!url.equalsIgnoreCase(null) && url.contains(MySQL)) {
                validateJdbcDriverVersion(metaData.getDriverVersion(), metaData.getJDBCMajorVersion(),
                        metaData.getJDBCMinorVersion());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void validateJdbcDriverVersion(String dName, int dMaxv, int dMinv) {
        if (dMaxv <= 3 && dMinv <= 0) {
            logErrorMessage(dName);
        }
    }

    protected void logErrorMessage(String dname) {
        if (logger.isErrorEnabled()) {
            logger.error((format("JDBC Driver " + dname + " "
                    + " version is not applicable for Migrator , Use latest version of the driver ")));
        }
        System.exit(DRIVER_ERROR);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public int getMinorVersion() {
        return minorVersion;
    }

    public void setMinorVersion(int minorVersion) {
        this.minorVersion = minorVersion;
    }

    public int getMajorVersion() {
        return majorVersion;
    }

    public void setMajorVersion(int majorVersion) {
        this.majorVersion = majorVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DriverInfo that = (DriverInfo) o;

        if (majorVersion != that.majorVersion)
            return false;
        if (minorVersion != that.minorVersion)
            return false;
        if (name != null ? !name.equals(that.name) : that.name != null)
            return false;
        if (version != null ? !version.equals(that.version) : that.version != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + minorVersion;
        result = 31 * result + majorVersion;
        return result;
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this);
    }
}
