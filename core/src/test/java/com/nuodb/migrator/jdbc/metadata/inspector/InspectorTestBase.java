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
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.nuodb.migrator.jdbc.dialect.Dialect;
import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.resolve.DatabaseInfo;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.testng.annotations.BeforeMethod;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import static com.nuodb.migrator.utils.ReflectionUtils.newInstance;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.MockitoAnnotations.initMocks;

/**
 * @author Sergey Bushik
 */
public class InspectorTestBase {

    @Spy
    private Dialect dialect;
    @Spy
    private Inspector inspector;
    @Mock
    private Connection connection;
    @Mock
    private DialectResolver dialectResolver;
    @InjectMocks
    private InspectionManager inspectionManager = new InspectionManager();

    protected InspectorTestBase(Class<? extends Inspector> inspectorClass) {
        this(inspectorClass, mock(Dialect.class));
    }

    protected InspectorTestBase(Class<? extends Inspector> inspectorClass, Dialect dialect) {
        this(newInstance(inspectorClass), dialect);
    }

    protected InspectorTestBase(Inspector inspector, Dialect dialect) {
        this.inspector = inspector;
        this.dialect = dialect;
    }

    @BeforeMethod
    public void setUp() throws Exception {
        initMocks(this);

        Dialect dialect = getDialect();
        DialectResolver dialectResolver = getDialectResolver();
        given(dialectResolver.resolve(any(Connection.class))).willReturn(dialect);
        given(dialectResolver.resolve(any(DatabaseInfo.class))).willReturn(dialect);
        given(dialectResolver.resolve(any(DatabaseMetaData.class))).willReturn(dialect);

        getInspectionManager().addInspector(getInspector());
    }

    public Dialect getDialect() {
        return dialect;
    }

    public Inspector getInspector() {
        return inspector;
    }

    public Connection getConnection() {
        return connection;
    }

    public DialectResolver getDialectResolver() {
        return dialectResolver;
    }

    public InspectionManager getInspectionManager() {
        return inspectionManager;
    }
}
