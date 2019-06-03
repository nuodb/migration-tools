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

import com.google.common.collect.Maps;
import com.nuodb.migrator.backup.format.csv.CsvFormat;
import com.nuodb.migrator.cli.parse.Parser;
import com.nuodb.migrator.cli.parse.parser.ParserImpl;
import com.nuodb.migrator.jdbc.commit.BatchCommitStrategy;
import com.nuodb.migrator.jdbc.query.InsertType;
import com.nuodb.migrator.spec.DriverConnectionSpec;
import com.nuodb.migrator.spec.LoadJobSpec;
import com.nuodb.migrator.spec.ResourceSpec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static com.nuodb.migrator.jdbc.JdbcConstants.NUODB_DRIVER;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;

/**
 * @author Sergey Bushik
 */
public class CliLoadJobTest {

    private Parser parser;
    private CliLoadJob cliLoadJob;

    @BeforeMethod
    public void setUp() {
        parser = spy(new ParserImpl());
        cliLoadJob = spy(new CliLoadJob());
    }

    @Test
    public void testParse() {
        String[] arguments = { "--target.url=jdbc:com.nuodb://localhost/test?schema=hockey", "--target.username=dba",
                "--target.password=goalie",

                "--input.path=/tmp/dump.cat", "--input.csv.encoding=cp1251", "--input.csv.delimiter= ",
                "--input.csv.quoting=true", "--input.csv.escape=|",

                "--table.deployments.insert", "--table.deployments_nodes.replace", "--time.zone=GMT+2" };
        parser.parse(arguments, cliLoadJob);

        assertEquals(cliLoadJob.getJobSpec(), createLoadSpec());
    }

    private LoadJobSpec createLoadSpec() {
        LoadJobSpec loadJobSpec = new LoadJobSpec();

        DriverConnectionSpec connectionSpec = new DriverConnectionSpec();
        connectionSpec.setDriver(NUODB_DRIVER);
        connectionSpec.setUrl("jdbc:com.nuodb://localhost/test?schema=hockey");
        connectionSpec.setUsername("dba");
        connectionSpec.setPassword("goalie");
        loadJobSpec.setTargetSpec(connectionSpec);

        ResourceSpec inputSpec = new ResourceSpec();
        inputSpec.setPath("/tmp/dump.cat");

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(CsvFormat.ATTRIBUTE_ENCODING, "cp1251");
        attributes.put(CsvFormat.ATTRIBUTE_DELIMITER, " ");
        attributes.put(CsvFormat.ATTRIBUTE_QUOTING, "true");
        attributes.put(CsvFormat.ATTRIBUTE_ESCAPE, "|");
        inputSpec.setAttributes(attributes);
        loadJobSpec.setInputSpec(inputSpec);
        loadJobSpec.setInsertType(InsertType.INSERT);

        Map<String, InsertType> tableInsertTypes = Maps.newHashMap();
        tableInsertTypes.put("deployments", InsertType.INSERT);
        tableInsertTypes.put("deployments_nodes", InsertType.REPLACE);
        loadJobSpec.setTableInsertTypes(tableInsertTypes);
        loadJobSpec.setTimeZone(TimeZone.getTimeZone("GMT+2"));
        loadJobSpec.setCommitStrategy(new BatchCommitStrategy());
        return loadJobSpec;
    }
}
