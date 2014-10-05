/**
 * Copyright (c) 2014, NuoDB, Inc.
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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static com.google.common.collect.Ordering.natural;

/**
 * @author Sergey Bushik
 */
public class NuoDBDatabaseInfo extends DatabaseInfo {

    private Integer protocolVersion;

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

    public NuoDBDatabaseInfo(String productName, String productVersion, Integer majorVersion,
                             Integer minorVersion) {
        super(productName, productVersion, majorVersion, minorVersion);
    }

    public NuoDBDatabaseInfo(DatabaseMetaData metaData) throws SQLException {
        super(metaData);
    }

    public NuoDBDatabaseInfo(String productName, String productVersion, Integer majorVersion,
                             Integer minorVersion, Integer protocolVersion) {
        super(productName, productVersion, majorVersion, minorVersion);
        setProtocolVersion(protocolVersion);
    }

    public Integer getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(Integer protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    protected ComparisonChain isAssignable(DatabaseInfo databaseInfo, ComparisonChain comparator) {
        comparator = isAssignableProductName(databaseInfo, comparator);
        comparator = isAssignableProductVersion(databaseInfo, comparator);
        comparator = isAssignableProtocolVersion(databaseInfo, comparator);
        comparator = isAssignableMajorVersion(databaseInfo, comparator);
        comparator = isAssignableMinorVersion(databaseInfo, comparator);
        return comparator;
    }

    protected ComparisonChain isAssignableProtocolVersion(DatabaseInfo databaseInfo, ComparisonChain comparator) {
        if (databaseInfo instanceof NuoDBDatabaseInfo) {
            Integer protocolVersion = ((NuoDBDatabaseInfo) databaseInfo).getProtocolVersion();
            return comparator.compare(getProtocolVersion(), protocolVersion, natural().nullsFirst());
        } else {
            return comparator;
        }
    }
}
