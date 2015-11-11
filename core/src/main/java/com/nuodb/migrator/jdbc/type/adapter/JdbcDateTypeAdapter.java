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
import com.nuodb.migrator.jdbc.type.JdbcTypeAdapterBase;

import java.sql.*;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class JdbcDateTypeAdapter extends JdbcTypeAdapterBase<Date> {

    public static final JdbcTypeAdapter INSTANCE = new JdbcDateTypeAdapter();

    public JdbcDateTypeAdapter() {
        super(Date.class);
    }

    @Override
    public <X> Date wrap(X value, Connection connection) throws SQLException {
        if (value == null) {
            return null;
        } else if (Long.class.isInstance(value)) {
            return new Date(((Long) value).longValue());
        } else if (Date.class.isInstance(value)) {
            return (Date) value;
        } else if (java.util.Date.class.isInstance(value)) {
            return new Date(((java.util.Date) value).getTime());
        } else if (Calendar.class.isInstance(value)) {
            return new Date(((Calendar) value).getTimeInMillis());
        } else {
            throw newWrapFailure(value);
        }
    }

    @Override
    public <X> X unwrap(Date value, Class<X> valueClass, Connection connection) throws SQLException {
        if (value == null) {
            return null;
        } else if (valueClass.isAssignableFrom(java.util.Date.class)) {
            return (X) value;
        } else if (valueClass.isAssignableFrom(Long.class)) {
            return (X) Long.valueOf(value.getTime());
        } else if (valueClass.isAssignableFrom(Date.class)) {
            return (X) value;
        } else if (valueClass.isAssignableFrom(Time.class)) {
            return (X) new Time(value.getTime());
        } else if (valueClass.isAssignableFrom(Timestamp.class)) {
            return (X) new Timestamp(value.getTime());
        } else if (valueClass.isAssignableFrom(Calendar.class)) {
            GregorianCalendar calendar = new GregorianCalendar();
            calendar.setTimeInMillis(value.getTime());
            return (X) calendar;
        } else {
            throw newUnwrapFailure(valueClass);
        }
    }
}
