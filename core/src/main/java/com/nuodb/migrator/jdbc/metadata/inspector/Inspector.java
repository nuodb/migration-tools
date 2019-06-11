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
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataHandler;
import com.nuodb.migrator.jdbc.query.Query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

/**
 * @author Sergey Bushik
 */
public interface Inspector<M extends MetaData, I extends InspectionScope> extends MetaDataHandler {

    void inspect(InspectionContext inspectionContext) throws SQLException;

    void inspectObject(InspectionContext inspectionContext, M object) throws SQLException;

    void inspectObjects(InspectionContext inspectionContext, Collection<? extends M> objects) throws SQLException;

    void inspectScope(InspectionContext inspectionContext, I inspectionScope) throws SQLException;

    void inspectScopes(InspectionContext inspectionContext, Collection<? extends I> inspectionScopes)
            throws SQLException;

    boolean supportsScope(InspectionContext inspectionContext, InspectionScope inspectionScope) throws SQLException;

    Statement createStatement(InspectionContext inspectionContext, I inspectionScope, Query query) throws SQLException;

    ResultSet openResultSet(InspectionContext inspectionContext, I inspectionScope, Query query, Statement statement)
            throws SQLException;

    void closeStatement(InspectionContext inspectionContext, I inspectionScope, Query query, Statement statement)
            throws SQLException;
}
