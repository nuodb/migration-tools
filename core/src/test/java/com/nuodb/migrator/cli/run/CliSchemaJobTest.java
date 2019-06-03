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

import com.google.common.collect.Sets;
import com.nuodb.migrator.cli.parse.Parser;
import com.nuodb.migrator.cli.parse.parser.ParserImpl;
import com.nuodb.migrator.jdbc.dialect.IdentifierNormalizers;
import com.nuodb.migrator.jdbc.dialect.IdentifierQuotings;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.generator.GroupScriptsBy;
import com.nuodb.migrator.jdbc.metadata.generator.ScriptType;
import com.nuodb.migrator.spec.DriverConnectionSpec;
import com.nuodb.migrator.spec.ResourceSpec;
import com.nuodb.migrator.spec.SchemaJobSpec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.nuodb.migrator.jdbc.JdbcConstants.NUODB_DRIVER;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;

/**
 * @author Sergey Bushik
 */
public class CliSchemaJobTest {

    private Parser parser;
    private CliSchemaJob cliSchemaJob;

    @BeforeMethod
    public void setUp() {
        parser = spy(new ParserImpl());
        cliSchemaJob = spy(new CliSchemaJob());
    }

    @Test
    public void testParse() {
        String[] arguments = { "--source.driver=oracle.jdbc.driver.OracleDriver",
                "--source.url=jdbc:oracle:thin:@//localhost:1521/test", "--source.username=test",
                "--source.password=12345", "--source.schema=test",

                "--target.url=jdbc:com.nuodb://localhost/test", "--target.username=dba", "--target.password=goalie",
                "--target.schema=test",

                "--output.path=/tmp/schema.sql",

                "--use.explicit.defaults=false", "--meta.data.check=false", "--meta.data.sequence=false",
                "--script.type=drop,create", "--group.scripts.by=meta.data", "--identifier.quoting=always",
                "--identifier.normalizer=standard", "--fail.on.empty.database=false", };
        parser.parse(arguments, cliSchemaJob);

        assertEquals(cliSchemaJob.getJobSpec(), createSchemaSpec());
    }

    private SchemaJobSpec createSchemaSpec() {
        SchemaJobSpec schemaSpec = new SchemaJobSpec();

        DriverConnectionSpec sourceConnectionSpec = new DriverConnectionSpec();
        sourceConnectionSpec.setDriver("oracle.jdbc.driver.OracleDriver");
        sourceConnectionSpec.setUrl("jdbc:oracle:thin:@//localhost:1521/test");
        sourceConnectionSpec.setUsername("test");
        sourceConnectionSpec.setPassword("12345");
        sourceConnectionSpec.setSchema("test");
        schemaSpec.setSourceSpec(sourceConnectionSpec);

        DriverConnectionSpec targetConnectionSpec = new DriverConnectionSpec();
        targetConnectionSpec.setDriver(NUODB_DRIVER);
        targetConnectionSpec.setUrl("jdbc:com.nuodb://localhost/test");
        targetConnectionSpec.setUsername("dba");
        targetConnectionSpec.setPassword("goalie");
        targetConnectionSpec.setSchema("test");
        schemaSpec.setTargetSpec(targetConnectionSpec);

        ResourceSpec outputSpec = new ResourceSpec();
        outputSpec.setPath("/tmp/schema.sql");
        schemaSpec.setOutputSpec(outputSpec);

        Collection<MetaDataType> metaDataTypes = newArrayList(MetaDataType.TYPES);
        metaDataTypes.remove(MetaDataType.CHECK);
        metaDataTypes.remove(MetaDataType.SEQUENCE);
        schemaSpec.setObjectTypes(metaDataTypes);

        schemaSpec.setScriptTypes(newHashSet(ScriptType.DROP, ScriptType.CREATE));
        schemaSpec.setGroupScriptsBy(GroupScriptsBy.META_DATA);
        schemaSpec.setIdentifierQuoting(IdentifierQuotings.ALWAYS);
        schemaSpec.setIdentifierNormalizer(IdentifierNormalizers.STANDARD);
        schemaSpec.setFailOnEmptyDatabase(false);
        return schemaSpec;
    }
}
