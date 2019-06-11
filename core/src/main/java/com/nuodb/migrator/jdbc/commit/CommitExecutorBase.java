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
package com.nuodb.migrator.jdbc.commit;

import com.nuodb.migrator.jdbc.query.Query;

import java.sql.Statement;

/**
 * @author Sergey Bushik
 */
public abstract class CommitExecutorBase<S extends Statement> implements CommitExecutor<S> {

    protected S statement;
    protected Query query;

    public CommitExecutorBase(S statement, Query query) {
        this.statement = statement;
        this.query = query;
    }

    @Override
    public S getStatement() {
        return statement;
    }

    @Override
    public Query getQuery() {
        return query;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CommitExecutorBase that = (CommitExecutorBase) o;

        if (query != null ? !query.equals(that.query) : that.query != null)
            return false;
        if (statement != null ? !statement.equals(that.statement) : that.statement != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = statement != null ? statement.hashCode() : 0;
        result = 31 * result + (query != null ? query.hashCode() : 0);
        return result;
    }
}
