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
package com.nuodb.migrator.jdbc.dialect;

import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.session.Session;

/**
 * @author Sergey Bushik
 */
public class SimpleScript implements Script, Comparable<Script> {

    private boolean literal;
    private String script;
    private Session session;
    private Dialect dialect;
    private DatabaseInfo databaseInfo;

    public SimpleScript(String script, DatabaseInfo databaseInfo) {
        this(script, databaseInfo, false);
    }

    /**
     * Constructs script detached from any database session
     *
     * @param script       source of the script
     * @param databaseInfo target database info
     * @param literal      true if script is literal
     */
    public SimpleScript(String script, DatabaseInfo databaseInfo, boolean literal) {
        this.script = script;
        this.databaseInfo = databaseInfo;
        this.literal = literal;
    }

    /**
     * Constructs script attached to the specified session
     *
     * @param script  source of the script
     * @param session source database session
     */
    public SimpleScript(String script, Session session) {
        this(script, session, false);
    }

    public SimpleScript(String script, Session session, boolean literal) {
        this.script = script;
        this.session = session;
        this.dialect = session.getDialect();
        this.databaseInfo = session.getDatabaseInfo();
        this.literal = literal;
    }

    @Override
    public String getScript() {
        return script;
    }

    @Override
    public Session getSession() {
        return session;
    }

    @Override
    public Dialect getDialect() {
        return dialect;
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return databaseInfo;
    }

    @Override
    public boolean isLiteral() {
        return literal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleScript)) return false;

        SimpleScript that = (SimpleScript) o;

        if (databaseInfo != null ? !databaseInfo.equals(that.getDatabaseInfo()) : that.getDatabaseInfo() != null)
            return false;
        if (script != null ? !script.equals(that.script) : that.script != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = script != null ? script.hashCode() : 0;
        result = 31 * result + (databaseInfo != null ? databaseInfo.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(Script o) {
        return String.CASE_INSENSITIVE_ORDER.compare(o.getScript(), getScript());
    }
}
