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

/**
 * @author Sergey Bushik
 */
public class SimpleDatabaseMatcher implements DatabaseMatcher {

    protected final String productName;
    protected final String productVersion;
    protected final Integer majorVersion;
    protected final Integer minorVersion;

    public SimpleDatabaseMatcher(String productName) {
        this(productName, null);
    }

    public SimpleDatabaseMatcher(String productName, String productVersion) {
        this(productName, productVersion, null);
    }

    public SimpleDatabaseMatcher(String productName, String productVersion, Integer majorVersion) {
        this(productName, productVersion, majorVersion, null);
    }

    public SimpleDatabaseMatcher(String productName, String productVersion, Integer majorVersion,
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
