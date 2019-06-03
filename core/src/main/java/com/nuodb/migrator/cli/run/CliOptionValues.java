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
package com.nuodb.migrator.cli.run;

import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import com.nuodb.migrator.jdbc.commit.BatchCommitStrategy;
import com.nuodb.migrator.jdbc.commit.CommitStrategy;
import com.nuodb.migrator.jdbc.commit.SingleCommitStrategy;
import com.nuodb.migrator.jdbc.dialect.IdentifierNormalizer;
import com.nuodb.migrator.jdbc.dialect.IdentifierQuoting;

import java.util.Map;
import java.util.TreeMap;

import static com.nuodb.migrator.jdbc.dialect.IdentifierNormalizers.*;
import static com.nuodb.migrator.jdbc.dialect.IdentifierQuotings.ALWAYS;
import static com.nuodb.migrator.jdbc.dialect.IdentifierQuotings.MINIMAL;
import static com.nuodb.migrator.utils.StringUtils.isEmpty;
import static java.lang.Integer.parseInt;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.sql.Connection.*;

/**
 * Defines symbolic, readable names for parameters values and resolution of a
 * name to a required constant.
 *
 * @author Sergey Bushik
 */
public class CliOptionValues {

    public static final CliOptionValues INSTANCE = new CliOptionValues();

    /**
     * Identifier quotings
     */
    public static final String IDENTIFIER_QUOTING_MINIMAL = "minimal";
    public static final String IDENTIFIER_QUOTING_ALWAYS = "always";

    /**
     * Identifier normalizer
     */
    public static final String IDENTIFIER_NORMALIZER_NOOP = "noop";
    public static final String IDENTIFIER_NORMALIZER_STANDARD = "standard";
    public static final String IDENTIFIER_NORMALIZER_LOWERCASE = "lower.case";
    public static final String IDENTIFIER_NORMALIZER_UPPERCASE = "upper.case";

    /**
     * Standard isolation levels
     */
    public static final String TRANSACTION_ISOLATION_NONE = "none";
    public static final String TRANSACTION_ISOLATION_READ_UNCOMMITTED = "read.uncommitted";
    public static final String TRANSACTION_ISOLATION_READ_COMMITTED = "read.committed";
    public static final String TRANSACTION_ISOLATION_REPEATABLE_READ = "repeatable.read";
    public static final String TRANSACTION_ISOLATION_SERIALIZABLE = "serializable";

    /**
     * NuoDB transaction isolation levels
     */
    public static final String TRANSACTION_ISOLATION_WRITE_COMMITTED = "write.committed";
    public static final String TRANSACTION_ISOLATION_CONSISTENT_READ = "consistent.read";

    /**
     * Naming strategies
     */
    public static final String NAMING_STRATEGY_QUALIFY = "qualify";
    public static final String NAMING_STRATEGY_HASH = "hash";
    public static final String NAMING_STRATEGY_AUTO = "auto";

    /**
     * Commit strategies
     */
    public static final String COMMIT_STRATEGY_SINGLE = "single";
    public static final String COMMIT_STRATEGY_BATCH = "batch";

    private CliOptionValues() {
    }

    public Map<String, IdentifierNormalizer> getIdentifierNormalizerMap() {
        Map<String, IdentifierNormalizer> identifierNormalizers = new TreeMap<String, IdentifierNormalizer>(
                CASE_INSENSITIVE_ORDER);
        identifierNormalizers.put(IDENTIFIER_NORMALIZER_NOOP, NOOP);
        identifierNormalizers.put(IDENTIFIER_NORMALIZER_STANDARD, STANDARD);
        identifierNormalizers.put(IDENTIFIER_NORMALIZER_LOWERCASE, LOWER_CASE);
        identifierNormalizers.put(IDENTIFIER_NORMALIZER_UPPERCASE, UPPER_CASE);
        return identifierNormalizers;
    }

    public Map<String, IdentifierQuoting> getIdentifierQuotingMap() {
        Map<String, IdentifierQuoting> identifierQuotings = new TreeMap<String, IdentifierQuoting>(
                CASE_INSENSITIVE_ORDER);
        identifierQuotings.put(IDENTIFIER_QUOTING_MINIMAL, MINIMAL);
        identifierQuotings.put(IDENTIFIER_QUOTING_ALWAYS, ALWAYS);
        return identifierQuotings;
    }

    /**
     * http://doc.nuodb.com/plugins/viewsource/viewpagesrc.action?pageId=18319037
     *
     * @return
     */
    public Map<String, Integer> getTransactionIsolationMap() {
        Map<String, Integer> transactionIsolations = new TreeMap<String, Integer>(CASE_INSENSITIVE_ORDER);
        // standard isolation levels
        transactionIsolations.put(TRANSACTION_ISOLATION_NONE, TRANSACTION_NONE);
        transactionIsolations.put(TRANSACTION_ISOLATION_READ_UNCOMMITTED, TRANSACTION_READ_UNCOMMITTED);
        transactionIsolations.put(TRANSACTION_ISOLATION_READ_COMMITTED, TRANSACTION_READ_COMMITTED);
        transactionIsolations.put(TRANSACTION_ISOLATION_REPEATABLE_READ, TRANSACTION_REPEATABLE_READ);
        transactionIsolations.put(TRANSACTION_ISOLATION_SERIALIZABLE, TRANSACTION_SERIALIZABLE);
        // NuoDB specific constants
        transactionIsolations.put(TRANSACTION_ISOLATION_WRITE_COMMITTED, 5);
        transactionIsolations.put(TRANSACTION_ISOLATION_CONSISTENT_READ, 7);
        return transactionIsolations;
    }

    /**
     * Parses transaction isolation value of a given option.
     *
     * @param option
     * @param value
     * @return integer constant of transaction isolation level or null
     */
    public Integer getTransactionIsolation(Option option, String value) {
        Integer transactionIsolation = null;
        if (!isEmpty(value)) {
            transactionIsolation = getTransactionIsolationMap().get(value);
            if (transactionIsolation == null) {
                try {
                    transactionIsolation = parseInt(value);
                } catch (NumberFormatException exception) {
                    throw new OptionException(exception.getMessage(), exception, option);
                }
            }
        }
        return transactionIsolation;
    }

    public Map<String, CommitStrategy> getCommitStrategyMap() {
        Map<String, CommitStrategy> commitStrategyMapping = new TreeMap<String, CommitStrategy>(CASE_INSENSITIVE_ORDER);
        commitStrategyMapping.put(COMMIT_STRATEGY_SINGLE, new SingleCommitStrategy());
        commitStrategyMapping.put(COMMIT_STRATEGY_BATCH, new BatchCommitStrategy());
        return commitStrategyMapping;
    }
}
