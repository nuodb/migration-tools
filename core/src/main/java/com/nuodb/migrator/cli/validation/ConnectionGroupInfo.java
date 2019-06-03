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
package com.nuodb.migrator.cli.validation;

/**
 * @author Sergey Bushik
 */
public class ConnectionGroupInfo {

    private String driverOption;
    private String urlOption;
    private String usernameOption;
    private String passwordOption;
    private String catalogOption;
    private String schemaOption;
    private String propertiesOption;
    private String transactionIsolationOption;

    public ConnectionGroupInfo(String driverOption, String urlOption, String usernameOption, String passwordOption,
            String catalogOption, String schemaOption, String propertiesOption, String transactionIsolationOption) {
        this.driverOption = driverOption;
        this.urlOption = urlOption;
        this.usernameOption = usernameOption;
        this.passwordOption = passwordOption;
        this.catalogOption = catalogOption;
        this.schemaOption = schemaOption;
        this.propertiesOption = propertiesOption;
        this.transactionIsolationOption = transactionIsolationOption;
    }

    public String getDriverOption() {
        return driverOption;
    }

    public String getUrlOption() {
        return urlOption;
    }

    public String getUsernameOption() {
        return usernameOption;
    }

    public String getPasswordOption() {
        return passwordOption;
    }

    public String getCatalogOption() {
        return catalogOption;
    }

    public String getSchemaOption() {
        return schemaOption;
    }

    public String getPropertiesOption() {
        return propertiesOption;
    }

    public String getTransactionIsolationOption() {
        return transactionIsolationOption;
    }
}
