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

import com.nuodb.migrator.backup.format.csv.CsvFormat;
import com.nuodb.migrator.cli.parse.Parser;
import com.nuodb.migrator.cli.parse.parser.ParserImpl;
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
    public void setUp() {
        parser = spy(new ParserImpl());
        cliDumpJob = spy(new CliDumpJob());
    }

    @Test
    public void testParse() {
        String[] arguments = { "--source.driver=com.mysql.jdbc.Driver", "--source.url=jdbc:mysql://localhost:3306/test",
                "--source.username=root", "--source.password=12345", "--source.catalog=test",
                "--source.properties=profileSQL=true",

                "--output.path=/tmp/dump.cat", "--output.type=xml",

                "--output.csv.encoding=cp1251", "--output.csv.delimiter=,", "--output.csv.quoting=true",
                "--output.csv.escape=|",

                "--table.type=TABLE", "--table.type=SYSTEM TABLE",

                "--query=SELECT id, name, definition FROM definitions", "--time.zone=GMT" };
        parser.parse(arguments, cliDumpJob);

        assertEquals(cliDumpJob.getJobSpec(), createDumpSpec());
    }

    private DumpJobSpec createDumpSpec() {
        DumpJobSpec dumpSpec = new DumpJobSpec();

        DriverConnectionSpec connectionSpec = new DriverConnectionSpec();
        connectionSpec.setDriver("com.mysql.jdbc.Driver");
        connectionSpec.setUrl("jdbc:mysql://localhost:3306/test");
        connectionSpec.setUsername("root");
        connectionSpec.setPassword("12345");
        connectionSpec.setCatalog("test");

        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("profileSQL", "true");
        connectionSpec.setProperties(properties);

        dumpSpec.setSourceSpec(connectionSpec);

        ResourceSpec outputSpec = new ResourceSpec();
        outputSpec.setType("xml");
        outputSpec.setPath("/tmp/dump.cat");

        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put(CsvFormat.ATTRIBUTE_ENCODING, "cp1251");
        attributes.put(CsvFormat.ATTRIBUTE_DELIMITER, ",");
        attributes.put(CsvFormat.ATTRIBUTE_QUOTING, "true");
        attributes.put(CsvFormat.ATTRIBUTE_ESCAPE, "|");
        outputSpec.setAttributes(attributes);

        dumpSpec.setOutputSpec(outputSpec);
        dumpSpec.setQuerySpecs(asList(new QuerySpec("SELECT id, name, definition FROM definitions")));
        dumpSpec.setTableTypes(new String[] { "TABLE", "SYSTEM TABLE" });
        dumpSpec.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dumpSpec;
    }
}
