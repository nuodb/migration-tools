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
package com.nuodb.migrator.spec;

/**
 * @author Sergey Bushik
 */
public class ConnectionSpecBase extends SpecBase implements ConnectionSpec {

    public static final Boolean DEFAULT_AUTO_COMMIT = false;

    private String catalog;
    private String schema;
    private Boolean autoCommit = DEFAULT_AUTO_COMMIT;
    private Integer transactionIsolation;

    @Override
    public String getCatalog() {
        return catalog;
    }

    @Override
    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public Boolean getAutoCommit() {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(Boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public Integer getTransactionIsolation() {
        return transactionIsolation;
    }

    @Override
    public void setTransactionIsolation(Integer transactionIsolation) {
        this.transactionIsolation = transactionIsolation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ConnectionSpecBase))
            return false;
        if (!super.equals(o))
            return false;

        ConnectionSpecBase that = (ConnectionSpecBase) o;

        if (autoCommit != null ? !autoCommit.equals(that.autoCommit) : that.autoCommit != null)
            return false;
        if (catalog != null ? !catalog.equals(that.catalog) : that.catalog != null)
            return false;
        if (schema != null ? !schema.equals(that.schema) : that.schema != null)
            return false;
        if (transactionIsolation != null ? !transactionIsolation.equals(that.transactionIsolation)
                : that.transactionIsolation != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (catalog != null ? catalog.hashCode() : 0);
        result = 31 * result + (schema != null ? schema.hashCode() : 0);
        result = 31 * result + (autoCommit != null ? autoCommit.hashCode() : 0);
        result = 31 * result + (transactionIsolation != null ? transactionIsolation.hashCode() : 0);
        return result;
    }
}
