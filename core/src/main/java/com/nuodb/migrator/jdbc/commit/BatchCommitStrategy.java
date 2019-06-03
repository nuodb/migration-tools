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
import com.nuodb.migrator.utils.ObjectUtils;

import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Map;

import static com.nuodb.migrator.utils.ValidationUtils.instanceOf;
import static java.lang.Long.parseLong;
import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author Sergey Bushik
 */
public class BatchCommitStrategy implements CommitStrategy {

    public static final String ATTRIBUTE_BATCH_SIZE = "batch.size";

    public static final long BATCH_SIZE = 1000;

    private long batchSize = BATCH_SIZE;

    @Override
    public void setAttributes(Map<String, Object> attributes) {
        Object batchSizeValue = attributes.get(ATTRIBUTE_BATCH_SIZE);
        if (batchSizeValue instanceof String && !isEmpty((String) batchSizeValue)) {
            setBatchSize(parseLong((String) batchSizeValue));
        }
    }

    @Override
    public CommitExecutor createCommitExecutor(Statement statement, Query query) {
        instanceOf(statement, PreparedStatement.class);
        return new CommitExecutorBase<PreparedStatement>((PreparedStatement) statement, query) {

            private long batches;
            private long batchSize = getBatchSize();

            @Override
            public boolean execute() throws SQLException {
                statement.addBatch();
                batches++;
                if (batches > batchSize) {
                    executeBatch();
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public void finish() throws SQLException {
                if (batches > 0) {
                    executeBatch();
                }
            }

            protected void executeBatch() throws SQLException {
                statement.executeBatch();
                statement.getConnection().commit();
                batches = 0;
            }
        };
    }

    public long getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        BatchCommitStrategy that = (BatchCommitStrategy) o;

        if (batchSize != that.batchSize)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (batchSize ^ (batchSize >>> 32));
    }

    @Override
    public String toString() {
        return ObjectUtils.toString(this);
    }
}
