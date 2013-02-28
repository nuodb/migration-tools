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
package com.nuodb.migrator.cli.run;

import com.nuodb.migrator.cli.parse.Parser;
import com.nuodb.migrator.cli.parse.parser.ParserImpl;
import com.nuodb.migrator.dump.DumpJobFactory;
import com.nuodb.migrator.resultset.format.csv.CsvAttributes;
import com.nuodb.migrator.spec.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;

/**
 * @author Sergey Bushik
 */
public class CliDumpJobTest {

    private Parser parser;
    private CliDumpJob cliDumpJob;

    @BeforeMethod
    public void init() {
        parser = spy(new ParserImpl());
        cliDumpJob = spy(new CliDumpJob());
    }

    @Test
    public void testParse() {
        String[] arguments = {
                "--source.driver=com.mysql.jdbc.Driver",
                "--source.url=jdbc:mysql://localhost:3306/test",
                "--source.username=root",
                "--source.password=",
                "--source.catalog=test",
                "--source.properties=profileSQL=true",

                "--output.path=/tmp/dump.cat",
                "--output.type=xml",

                "--output.csv.encoding=cp1251",
                "--output.csv.delimiter=,",
                "--output.csv.quoting=true",
                "--output.csv.escape=|",

                "--table.type=TABLE",
                "--table.type=SYSTEM TABLE",
                "--table.users",
                "--table.users_roles.filter=role_id IN (1,2,3,4,5)",

                "--query=SELECT id, name, definition FROM definitions",
                "--time.zone=GMT"
        };
        parser.parse(arguments, cliDumpJob);

        DumpJobFactory dumpJobFactory = (DumpJobFactory) cliDumpJob.getJobFactory();
        assertEquals(dumpJobFactory.getDumpSpec(), createDumpSpec());
    }

    private DumpSpec createDumpSpec() {
        DumpSpec dumpSpec = new DumpSpec();

        JdbcConnectionSpec connectionSpec = new JdbcConnectionSpec();
        connectionSpec.setDriverClassName("com.mysql.jdbc.Driver");
        connectionSpec.setUrl("jdbc:mysql://localhost:3306/test");
        connectionSpec.setUsername("root");
        connectionSpec.setCatalog("test");

        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("profileSQL", "true");
        connectionSpec.setProperties(properties);

        dumpSpec.setConnectionSpec(connectionSpec);

        ResourceSpec outputSpec = new ResourceSpec();
        outputSpec.setType("xml");
        outputSpec.setPath("/tmp/dump.cat");

        Map<String, Object> attributes = new HashMap<String, Object>();
        properties.put(CsvAttributes.ATTRIBUTE_ENCODING, "cp1251");
        properties.put(CsvAttributes.ATTRIBUTE_DELIMITER, ",");
        properties.put(CsvAttributes.ATTRIBUTE_QUOTING, "true");
        properties.put(CsvAttributes.ATTRIBUTE_ESCAPE, "escape");
        outputSpec.setAttributes(attributes);

        dumpSpec.setOutputSpec(outputSpec);
        dumpSpec.setSelectQuerySpecs(asList(
                new SelectQuerySpec("users"),
                new SelectQuerySpec("users_roles", "role_id IN (1,2,3,4,5)")
        ));
        dumpSpec.setNativeQuerySpecs(
                asList(new NativeQuerySpec("SELECT id, name, definition FROM definitions")
                ));
        dumpSpec.setTableTypes(asList("TABLE", "SYSTEM TABLE"));
        dumpSpec.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dumpSpec;
    }
}
