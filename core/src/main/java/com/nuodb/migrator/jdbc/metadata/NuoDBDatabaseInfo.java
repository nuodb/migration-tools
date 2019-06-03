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
import com.nuodb.migrator.jdbc.query.StatementAction;
import com.nuodb.migrator.jdbc.query.StatementFactory;
import com.nuodb.migrator.jdbc.query.StatementTemplate;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.google.common.collect.Ordering.natural;

/**
 * @author Sergey Bushik
 */
public class NuoDBDatabaseInfo extends DatabaseInfo {

    private static final String QUERY = "SELECT GETEFFECTIVEPLATFORMVERSION() FROM DUAL";

    private Integer platformVersion;

    public NuoDBDatabaseInfo() {
    }

    public NuoDBDatabaseInfo(String productName) {
        super(productName);
    }

    public NuoDBDatabaseInfo(String productName, String productVersion) {
        super(productName, productVersion);
    }

    public NuoDBDatabaseInfo(String productName, String productVersion, Integer majorVersion) {
        super(productName, productVersion, majorVersion);
    }

    public NuoDBDatabaseInfo(String productName, String productVersion, Integer majorVersion, Integer minorVersion) {
        super(productName, productVersion, majorVersion, minorVersion);
    }

    public NuoDBDatabaseInfo(DatabaseMetaData metaData) throws SQLException {
        super(metaData);
        this.platformVersion = new StatementTemplate(metaData.getConnection())
                .executeStatement(new StatementFactory<Statement>() {
                    @Override
                    public Statement createStatement(Connection connection) throws SQLException {
                        return connection.createStatement();
                    }
                }, new StatementAction<Statement, Integer>() {
                    @Override
                    public Integer executeStatement(Statement statement) throws SQLException {
                        ResultSet resultSet = statement.executeQuery(QUERY);
                        return resultSet.next() ? resultSet.getInt(1) : null;
                    }
                });
    }

    public NuoDBDatabaseInfo(DatabaseMetaData metaData, Integer platformVersion) throws SQLException {
        super(metaData);
        this.platformVersion = platformVersion;
    }

    public NuoDBDatabaseInfo(String productName, String productVersion, Integer majorVersion, Integer minorVersion,
            Integer platformVersion) {
        super(productName, productVersion, majorVersion, minorVersion);
        setPlatformVersion(platformVersion);
    }

    public NuoDBDatabaseInfo(DatabaseInfo databaseInfo, Integer platformVersion) {
        super(databaseInfo.getProductName(), databaseInfo.getProductVersion(), databaseInfo.getMajorVersion(),
                databaseInfo.getMinorVersion());
        setPlatformVersion(platformVersion);
    }

    public Integer getPlatformVersion() {
        return platformVersion;
    }

    public void setPlatformVersion(Integer platformVersion) {
        this.platformVersion = platformVersion;
    }

    protected ComparisonChain isAssignable(DatabaseInfo databaseInfo, ComparisonChain comparator) {
        comparator = isAssignableProductName(databaseInfo, comparator);
        comparator = isAssignableProductVersion(databaseInfo, comparator);
        comparator = isAssignablePlatformVersion(databaseInfo, comparator);
        comparator = isAssignableMajorVersion(databaseInfo, comparator);
        comparator = isAssignableMinorVersion(databaseInfo, comparator);
        return comparator;
    }

    protected ComparisonChain isAssignablePlatformVersion(DatabaseInfo databaseInfo, ComparisonChain comparator) {
        if (databaseInfo instanceof NuoDBDatabaseInfo) {
            Integer platformVersion = ((NuoDBDatabaseInfo) databaseInfo).getPlatformVersion();
            return comparator.compare(getPlatformVersion(), platformVersion, natural().nullsFirst());
        } else {
            return comparator;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        NuoDBDatabaseInfo that = (NuoDBDatabaseInfo) o;

        if (platformVersion != null ? !platformVersion.equals(that.platformVersion) : that.platformVersion != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (platformVersion != null ? platformVersion.hashCode() : 0);
        return result;
    }
}
