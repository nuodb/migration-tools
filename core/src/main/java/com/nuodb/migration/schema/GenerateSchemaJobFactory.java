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
package com.nuodb.migration.schema;

import com.nuodb.migration.job.JobFactory;

import java.sql.ResultSet;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * @author Sergey Bushik
 */
public class GenerateSchemaJobFactory implements JobFactory<GenerateSchemaJob> {

    @Override
    public GenerateSchemaJob createJob() {
        GenerateSchemaJob job = new GenerateSchemaJob();

//        --time.zone=<time zone code>,optional;
//
//        ResultSet resultSet;
//        Date date = resultSet.getTime(column) (or resultSet.getDate(column) or resultSet.getTimestamp(column));
//
//        String timeZoneValue == <--time.zone>;
//        if (timeZoneValue != null) {
//            <dump date as is>;
//        } else {
//            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone( < timeZoneValue >));
//
//
//        }

        return job;
    }

    public static void main(String[] args) {
        Date now = new Date();
        System.out.println(now);
        Calendar calendar = Calendar.getInstance(
                TimeZone.getTimeZone("America/New_York"));
        calendar.setTimeInMillis(now.getTime());

        ResultSet resultSet;
        // PreparedStatement preparedStatement;
        // preparedStatement.setDate

        System.out.println(calendar.toString());
        System.out.println(calendar.getTime());

    }
}