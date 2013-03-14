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
package com.nuodb.migrator.jdbc.type.access;

import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;
import com.nuodb.migrator.jdbc.type.JdbcTypeException;
import com.nuodb.migrator.jdbc.type.adapter.JdbcTimestampTypeAdapter;
import com.nuodb.migrator.jdbc.type.jdbc2.JdbcTimestampType;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class JdbcTypeValueAccessTest {

    private static final int COLUMN = 1;

    @Spy
    private JdbcTypeValueAccessProvider jdbcTypeValueAccessProvider;
    @Mock
    private Connection connection;
    @Mock
    private ResultSet resultSet;
    @Mock
    private ResultSetMetaData metaData;
    @Mock
    private PreparedStatement preparedStatement;

    /**
     * Initializes mocks on this test instance & adds single TIMESTAMP type & TIMESTAMP type adapter to the {@link
     * JdbcTypeValueAccessProvider}
     *
     * @throws Exception
     */
    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        jdbcTypeValueAccessProvider.addJdbcType(JdbcTimestampType.INSTANCE);
        jdbcTypeValueAccessProvider.addJdbcTypeAdapter(JdbcTimestampTypeAdapter.INSTANCE);

        given(metaData.getColumnType(COLUMN)).willReturn(Types.TIMESTAMP);
        given(resultSet.getMetaData()).willReturn(metaData);
        given(resultSet.getStatement()).willReturn(preparedStatement);
        given(preparedStatement.getMetaData()).willReturn(metaData);
        given(preparedStatement.getConnection()).willReturn(connection);
    }

    @DataProvider(name = "getJdbcTypeValueGetter")
    public Object[][] createGetJdbcTypeValueGetterData() {
        return new Object[][]{
                {new JdbcTypeDesc(Types.TIMESTAMP)},
                {new JdbcTypeDesc(Types.TIMESTAMP, "TIMESTAMP")},
                {JdbcTimestampType.INSTANCE.getTypeDesc()},
        };
    }

    @Test(dataProvider = "getJdbcTypeValueGetter")
    public void testGetJdbcTypeValueGetter(JdbcTypeDesc jdbcTypeDesc) {
        JdbcTypeValueGetter<Object> jdbcTypeValueGetter = jdbcTypeValueAccessProvider.getJdbcTypeValueGetter(
                jdbcTypeDesc);
        assertNotNull(jdbcTypeValueGetter);
        verify(jdbcTypeValueAccessProvider).getJdbcTypeRequired(jdbcTypeDesc);
    }

    @DataProvider(name = "getJdbcTypeValueGetterFailed")
    public Object[][] createGetJdbcTypeValueGetterFailedData() {
        return new Object[][]{
                {new JdbcTypeDesc(Types.DATE)},
                {new JdbcTypeDesc(Types.TIME)}
        };
    }

    /**
     * Verifiers that the provider throws {@link JdbcTypeException} for the type descriptors which has now
     * associated {@link com.nuodb.migrator.jdbc.type.JdbcType} classes.
     *
     * @param jdbcTypeDesc to be verified
     */
    @Test(dataProvider = "getJdbcTypeValueGetterFailed", expectedExceptions = {JdbcTypeException.class})
    public void testGetJdbcTypeValueGetterFailed(JdbcTypeDesc jdbcTypeDesc) {
        jdbcTypeValueAccessProvider.getJdbcTypeValueGetter(jdbcTypeDesc);
    }

    @Test
    public void testResultSetAccess() throws Exception {
        Calendar calendar = Calendar.getInstance();
        given(resultSet.getTimestamp(COLUMN)).willReturn(new Timestamp(calendar.getTimeInMillis()));

        JdbcTypeValueAccess<Timestamp> resultSetAccess = jdbcTypeValueAccessProvider.getResultSetAccess(resultSet,
                COLUMN);

        Map<String, Object> options = new HashMap<String, Object>();
        assertEquals(resultSetAccess.getValue(options), new Timestamp(calendar.getTimeInMillis()));
        assertEquals(resultSetAccess.getValue(Calendar.class, options), calendar);
        verify(resultSet, times(2)).getTimestamp(COLUMN);
    }

    @Test
    public void testPreparedStatementAccess() throws Exception {
        Calendar calendar = Calendar.getInstance();
        given(resultSet.getTimestamp(COLUMN)).willReturn(new Timestamp(calendar.getTimeInMillis()));

        JdbcTypeValueAccess<Object> preparedStatementAccess =
                jdbcTypeValueAccessProvider.getPreparedStatementAccess(preparedStatement, COLUMN);

        Map<String, Object> options = new HashMap<String, Object>();
        preparedStatementAccess.setValue(calendar.getTimeInMillis(), options);
        preparedStatementAccess.setValue(calendar, options);
        verify(preparedStatement, times(2)).setTimestamp(eq(COLUMN), eq(new Timestamp(calendar.getTimeInMillis())));
    }
}
