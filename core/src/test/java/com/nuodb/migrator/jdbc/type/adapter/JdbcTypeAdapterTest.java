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
package com.nuodb.migrator.jdbc.type.adapter;

import com.nuodb.migrator.jdbc.type.JdbcTypeAdapter;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Calendar;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.type.MockTypes.*;
import static com.nuodb.migrator.utils.StreamUtils.newEqualsInputStream;
import static com.nuodb.migrator.utils.StreamUtils.newEqualsReader;
import static org.apache.commons.lang3.time.DateUtils.toCalendar;
import static org.mockito.BDDMockito.given;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings({ "unchecked", "SuspiciousToArrayCall" })
public class JdbcTypeAdapterTest {

    @Mock
    private Connection connection;

    @BeforeMethod
    public void setUp() throws SQLException {
        initMocks(this);
        given(connection.createBlob()).will(new Answer<Blob>() {
            @Override
            public Blob answer(InvocationOnMock invocation) throws Throwable {
                return newBlob();
            }
        });
        given(connection.createClob()).will(new Answer<Clob>() {
            @Override
            public Clob answer(InvocationOnMock invocation) throws Throwable {
                return newClob();
            }
        });
        given(connection.createNClob()).will(new Answer<NClob>() {
            @Override
            public NClob answer(InvocationOnMock invocation) throws Throwable {
                return newNClob();
            }
        });
    }

    @DataProvider(name = "wrap")
    public Object[][] createWrapData() throws Exception {
        List<Object> data = newArrayList();

        BigDecimal bigDecimal = new BigDecimal("12345");
        data.addAll(newArrayList(new Object[][] { { JdbcBigDecimalTypeAdapter.INSTANCE, null, null },
                { JdbcBigDecimalTypeAdapter.INSTANCE, bigDecimal, bigDecimal },
                { JdbcBigDecimalTypeAdapter.INSTANCE, new BigInteger("12345"), bigDecimal },
                { JdbcBigDecimalTypeAdapter.INSTANCE, 12345L, bigDecimal },
                { JdbcBigDecimalTypeAdapter.INSTANCE, "12345", bigDecimal } }));

        BigInteger bigInteger = new BigInteger("123");
        data.addAll(newArrayList(new Object[][] { { JdbcBigIntegerTypeAdapter.INSTANCE, null, null },
                { JdbcBigIntegerTypeAdapter.INSTANCE, bigInteger, bigInteger },
                { JdbcBigIntegerTypeAdapter.INSTANCE, new BigDecimal("123"), bigInteger },
                { JdbcBigIntegerTypeAdapter.INSTANCE, 123L, bigInteger },
                { JdbcBigIntegerTypeAdapter.INSTANCE, "123", bigInteger }, }));

        Blob blob = newBlob(new byte[] { (byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE });
        data.addAll(newArrayList(new Object[][] { { JdbcBlobTypeAdapter.INSTANCE, null, null },
                { JdbcBlobTypeAdapter.INSTANCE, new byte[] { (byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE },
                        blob },
                { JdbcBlobTypeAdapter.INSTANCE,
                        new ByteArrayInputStream(new byte[] { (byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE }),
                        blob } }));

        Clob clob = newClob("text");
        data.addAll(newArrayList(new Object[][] { { JdbcClobTypeAdapter.INSTANCE, null, null },
                { JdbcClobTypeAdapter.INSTANCE, "text", clob },
                { JdbcClobTypeAdapter.INSTANCE, "text".toCharArray(), clob },
                { JdbcClobTypeAdapter.INSTANCE, new StringReader("text"), clob },
                { JdbcClobTypeAdapter.INSTANCE, new ByteArrayInputStream("text".getBytes()), clob }, }));

        java.sql.Date date = new java.sql.Date(946677600000L);
        data.addAll(newArrayList(new Object[][] { { JdbcDateTypeAdapter.INSTANCE, null, null },
                { JdbcDateTypeAdapter.INSTANCE, 946677600000L, date },
                { JdbcDateTypeAdapter.INSTANCE, new java.sql.Date(946677600000L), date },
                { JdbcDateTypeAdapter.INSTANCE, new java.util.Date(946677600000L), date },
                { JdbcDateTypeAdapter.INSTANCE, toCalendar(date), date }, }));

        Timestamp timestamp = new Timestamp(946716300123L);
        data.addAll(newArrayList(new Object[][] { { JdbcTimestampTypeAdapter.INSTANCE, null, null },
                { JdbcTimestampTypeAdapter.INSTANCE, 946716300123L, timestamp },
                { JdbcTimestampTypeAdapter.INSTANCE, new Timestamp(946716300123L), timestamp },
                { JdbcTimestampTypeAdapter.INSTANCE, new java.util.Date(946716300123L), timestamp },
                { JdbcTimestampTypeAdapter.INSTANCE, toCalendar(new java.util.Date(946716300123L)), timestamp }, }));

        Time time = new Time(27900000L);
        data.addAll(newArrayList(new Object[][] { { JdbcTimeTypeAdapter.INSTANCE, null, null },
                { JdbcTimeTypeAdapter.INSTANCE, 27900000L, time },
                { JdbcTimeTypeAdapter.INSTANCE, new Time(27900000L), time },
                { JdbcTimeTypeAdapter.INSTANCE, new java.util.Date(27900000L), time },
                { JdbcTimeTypeAdapter.INSTANCE, toCalendar(new java.util.Date(27900000L)), time }, }));
        return data.toArray(new Object[data.size()][]);
    }

    @Test(dataProvider = "wrap")
    public void testWrap(JdbcTypeAdapter jdbcTypeAdapter, Object value, Object result) throws Exception {
        assertEquals(jdbcTypeAdapter.wrap(value, connection), result);
    }

    @DataProvider(name = "unwrap")
    public Object[][] createUnwrapData() throws Exception {
        List<Object> data = newArrayList();

        BigDecimal bigDecimal = new BigDecimal("54321.123");
        data.addAll(newArrayList(new Object[][] { { JdbcBigDecimalTypeAdapter.INSTANCE, null, BigDecimal.class, null },
                { JdbcBigDecimalTypeAdapter.INSTANCE, bigDecimal, BigDecimal.class, new BigDecimal("54321.123") },
                { JdbcBigDecimalTypeAdapter.INSTANCE, bigDecimal, BigInteger.class, new BigInteger("54321") },
                { JdbcBigDecimalTypeAdapter.INSTANCE, bigDecimal, Double.class, Double.valueOf("54321.123") },
                { JdbcBigDecimalTypeAdapter.INSTANCE, bigDecimal, Float.class, Float.valueOf("54321.123") },
                { JdbcBigDecimalTypeAdapter.INSTANCE, bigDecimal, Long.class, 54321L },
                { JdbcBigDecimalTypeAdapter.INSTANCE, bigDecimal, Integer.class, 54321 },
                { JdbcBigDecimalTypeAdapter.INSTANCE, bigDecimal, Short.class, (short) 54321 },
                { JdbcBigDecimalTypeAdapter.INSTANCE, bigDecimal, Byte.class, (byte) 54321 },
                { JdbcBigDecimalTypeAdapter.INSTANCE, bigDecimal, String.class, "54321.123" }, }));

        BigInteger bigInteger = new BigInteger("54321");
        data.addAll(newArrayList(new Object[][] { { JdbcBigIntegerTypeAdapter.INSTANCE, null, BigDecimal.class, null },
                { JdbcBigIntegerTypeAdapter.INSTANCE, bigInteger, BigDecimal.class, new BigDecimal("54321") },
                { JdbcBigIntegerTypeAdapter.INSTANCE, bigInteger, BigInteger.class, new BigInteger("54321") },
                { JdbcBigIntegerTypeAdapter.INSTANCE, bigInteger, Double.class, Double.valueOf("54321") },
                { JdbcBigIntegerTypeAdapter.INSTANCE, bigInteger, Float.class, Float.valueOf("54321") },
                { JdbcBigIntegerTypeAdapter.INSTANCE, bigInteger, Long.class, 54321L },
                { JdbcBigIntegerTypeAdapter.INSTANCE, bigInteger, Integer.class, 54321 },
                { JdbcBigIntegerTypeAdapter.INSTANCE, bigInteger, Short.class, (short) 54321 },
                { JdbcBigIntegerTypeAdapter.INSTANCE, bigInteger, Byte.class, (byte) 54321 },
                { JdbcBigIntegerTypeAdapter.INSTANCE, bigInteger, String.class, "54321" }, }));

        Blob blob = newBlob(new byte[] { (byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE });
        data.addAll(newArrayList(new Object[][] { { JdbcBlobTypeAdapter.INSTANCE, null, Blob.class, null },
                { JdbcBlobTypeAdapter.INSTANCE, blob, Blob.class,
                        newBlob(new byte[] { (byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE }) },
                { JdbcBlobTypeAdapter.INSTANCE, blob, byte[].class,
                        new byte[] { (byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE } },
                { JdbcBlobTypeAdapter.INSTANCE, blob, InputStream.class, newEqualsInputStream(new ByteArrayInputStream(
                        new byte[] { (byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE })) } }));

        Clob clob = newClob("text");
        data.addAll(newArrayList(new Object[][] { { JdbcClobTypeAdapter.INSTANCE, null, Clob.class, null },
                { JdbcClobTypeAdapter.INSTANCE, clob, char[].class, "text".toCharArray() },
                { JdbcClobTypeAdapter.INSTANCE, clob, String.class, "text" },
                { JdbcClobTypeAdapter.INSTANCE, clob, Reader.class, newEqualsReader(new StringReader("text")) },
                { JdbcClobTypeAdapter.INSTANCE, clob, InputStream.class,
                        newEqualsInputStream(new ByteArrayInputStream("text".getBytes())) } }));

        java.sql.Date date = new java.sql.Date(946677600000L);
        data.addAll(newArrayList(new Object[][] { { JdbcDateTypeAdapter.INSTANCE, null, java.sql.Date.class, null },
                { JdbcDateTypeAdapter.INSTANCE, date, java.util.Date.class, new java.util.Date(946677600000L) },
                { JdbcDateTypeAdapter.INSTANCE, date, java.sql.Date.class, new java.sql.Date(946677600000L) },
                { JdbcDateTypeAdapter.INSTANCE, date, Time.class, new Time(946677600000L) },
                { JdbcDateTypeAdapter.INSTANCE, date, Timestamp.class, new Timestamp(946677600000L) },
                { JdbcDateTypeAdapter.INSTANCE, date, Calendar.class, toCalendar(new java.util.Date(946677600000L)) },

        }));

        Timestamp timestamp = new Timestamp(946716300123L);
        data.addAll(newArrayList(new Object[][] { { JdbcTimestampTypeAdapter.INSTANCE, null, Timestamp.class, null },
                { JdbcTimestampTypeAdapter.INSTANCE, timestamp, Long.class, 946716300123L },
                { JdbcTimestampTypeAdapter.INSTANCE, timestamp, Timestamp.class, new Timestamp(946716300123L) },
                { JdbcTimestampTypeAdapter.INSTANCE, timestamp, java.util.Date.class,
                        new java.util.Date(946716300123L) },
                { JdbcTimestampTypeAdapter.INSTANCE, timestamp, java.sql.Date.class, new java.sql.Date(946716300123L) },
                { JdbcTimestampTypeAdapter.INSTANCE, timestamp, Calendar.class,
                        toCalendar(new java.util.Date(946716300123L)) }, }));

        Time time = new Time(23445000L);
        data.addAll(newArrayList(new Object[][] { { JdbcTimeTypeAdapter.INSTANCE, null, Time.class, null },
                { JdbcTimeTypeAdapter.INSTANCE, time, Long.class, 23445000L },
                { JdbcTimeTypeAdapter.INSTANCE, time, Time.class, new Time(23445000L) },
                { JdbcTimeTypeAdapter.INSTANCE, time, Timestamp.class, new Timestamp(23445000L) },
                { JdbcTimeTypeAdapter.INSTANCE, time, java.util.Date.class, new java.util.Date(23445000L) },
                { JdbcTimeTypeAdapter.INSTANCE, time, java.sql.Date.class, new java.sql.Date(23445000L) },
                { JdbcTimeTypeAdapter.INSTANCE, time, Calendar.class, toCalendar(new java.util.Date(23445000L)) }, }));
        return data.toArray(new Object[data.size()][]);
    }

    @Test(dataProvider = "unwrap")
    public void testUnwrap(JdbcTypeAdapter jdbcTypeAdapter, Object value, Class<?> valueClass, Object result)
            throws Exception {
        assertEquals(jdbcTypeAdapter.unwrap(value, valueClass, connection), result);
    }
}
