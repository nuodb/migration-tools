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
package com.nuodb.migrator.jdbc.type;

import com.nuodb.migrator.jdbc.model.Field;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * @author Sergey Bushik
 */
public class JdbcTypeValueTest {

    private static final int COLUMN = 1;
    private JdbcTypeValueBase<Object> jdbcTypeBase;

    @BeforeMethod
    public void setUp() {
        jdbcTypeBase = spy(new JdbcTypeValueBase<Object>(Types.OTHER, Object.class) {
            @Override
            public Object getValue(ResultSet resultSet, int index, Field field, Map<String, Object> options)
                    throws SQLException {
                return null;
            }

            @Override
            protected void setNullSafeValue(PreparedStatement statement, Object value, int index, Field field,
                    Map<String, Object> options) throws SQLException {
            }
        });
    }

    @Test
    public void testSetNullValue() throws Exception {
        PreparedStatement statement = mock(PreparedStatement.class);
        jdbcTypeBase.setValue(statement, COLUMN, null, null, null);
        verify(jdbcTypeBase).setNullValue(statement, COLUMN, null);
    }

    @Test
    public void testSetNullSafeValue() throws Exception {
        PreparedStatement statement = mock(PreparedStatement.class);
        Object value = new Object();
        jdbcTypeBase.setValue(statement, COLUMN, null, value, null);
        verify(jdbcTypeBase).setNullSafeValue(statement, value, COLUMN, null, null);
    }
}
