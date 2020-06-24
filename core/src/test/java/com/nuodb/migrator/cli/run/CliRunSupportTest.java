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
import com.nuodb.migrator.cli.parse.Group;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import com.nuodb.migrator.cli.parse.OptionSet;
import com.nuodb.migrator.cli.parse.Parser;
import com.nuodb.migrator.cli.parse.parser.ParserImpl;
import com.nuodb.migrator.jdbc.JdbcConstants;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilter;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilterManager;
import com.nuodb.migrator.spec.DriverConnectionSpec;
import com.nuodb.migrator.spec.ResourceSpec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;

import static com.beust.jcommander.internal.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.nuodb.migrator.jdbc.metadata.MetaDataUtils.createTable;
import static java.util.TimeZone.getTimeZone;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.*;
import static org.testng.Assert.assertEquals;

/**
 * Verifies functionality of the create & parse pairs of methods from
 * {@link CliRunSupport} for creating option & parsing that option to the
 * required spec classes. Correspondingly tested pairs are:
 * <ul>
 * <li>{@link CliRunSupport#createSourceGroup()} &
 * {@link CliRunSupport#parseSourceGroup(OptionSet, Option)}</li>
 * <li>{@link CliRunSupport#createTargetGroup()} &
 * {@link CliRunSupport#parseTargetGroup(OptionSet, Option)}</li>
 * <li>{@link CliRunSupport#createInputGroup()} &
 * {@link CliRunSupport#parseInputGroup(OptionSet, Option)}</li>
 * <li>{@link CliRunSupport#createOutputGroup()} &
 * {@link CliRunSupport#parseOutputGroup(OptionSet, Option)}</li>
 * <li>{@link CliRunSupport#createTimeZoneOption()} &
 * {@link CliRunSupport#parseTimeZoneOption(OptionSet, Option)}</li>
 * <li>{@link com.nuodb.migrator.cli.run.CliRunSupport#createMetaDataFilterManagerGroup()}
 * &
 * {@link CliRunSupport#parseMetaDataFilterManagerGroup (OptionSet, ScriptGeneratorJobSpecBase, Option)}</li>
 * </ul>
 *
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
@Test(groups = "cli.run.support")
public class CliRunSupportTest {

    private Parser parser;
    private CliRunSupport cliRunSupport;

    @BeforeMethod
    public void setUp() {
        parser = spy(new ParserImpl());
        cliRunSupport = spy(new CliRunSupport());
    }

    @DataProvider(name = "sourceGroup")
    public Object[][] createSourceGroupData() {
        String[] arguments = { "--source.driver=com.mysql.jdbc.Driver", "--source.url=jdbc:mysql://localhost:3306/test",
                "--source.username=root", "--source.password=12345", "--source.catalog=test",
                "--source.properties=profileSQL=true" };

        DriverConnectionSpec expected = new DriverConnectionSpec();
        expected.setDriver("com.mysql.jdbc.Driver");
        expected.setUrl("jdbc:mysql://localhost:3306/test");
        expected.setUsername("root");
        expected.setPassword("12345");
        expected.setCatalog("test");

        Map<String, Object> properties = Maps.newHashMap();
        properties.put("profileSQL", "true");
        expected.setProperties(properties);

        return new Object[][] { { arguments, expected } };
    }

    @Test(dataProvider = "sourceGroup")
    public void testSourceGroup(String[] arguments, DriverConnectionSpec expected) {
        Group group = cliRunSupport.createSourceGroup();
        assertNotNull(group, "Source group of options is required");

        OptionSet options = parser.parse(arguments, group);
        assertNotNull(options, "Option set containing source group options is expected");

        DriverConnectionSpec actual = cliRunSupport.parseSourceGroup(options, group);
        assertNotNull(actual, "Connection specification is expected");
        assertEquals(actual, expected);
    }

    @DataProvider(name = "sourceGroupValidation")
    public Object[][] createSourceGroupValidationData() {
        return new Object[][] { {
                new String[] { "--source.driver=com.mysql.jdbc.Driver", "--source.url=jdbc:mysql://localhost:3306/test",
                        "--source.username=root", "--source.catalog=catalog", "--source.schema=schema" },
                "MySQL does not support source.schema option" },
                { new String[] { "--source.driver=com.nuodb.jdbc.Driver",
                        "--source.url=jdbc:com.nuodb://localhost/test", "--source.catalog=test",
                        "--source.password=goalie", "--source.schema=schema" }, "NuoDB doesn't source.catalog option" },
                { new String[] { "--source.driver=com.nuodb.jdbc.Driver",
                        "--source.url=jdbc:com.nuodb://localhost/test", "--source.password=goalie",
                        "--source.schema=schema" }, "NuoDB requires source.username option" },
                { new String[] { "--source.driver=com.nuodb.jdbc.Driver",
                        "--source.url=jdbc:com.nuodb://localhost/test", "--source.username=dba",
                        "--source.schema=schema" }, "NuoDB requires source.password option" },
                { new String[] { "--source.driver=oracle.jdbc.driver.OracleDriver",
                        "--source.url=jdbc:oracle:thin:@//localhost/test", "--source.catalog=catalog" },
                        "Oracle doesn't support source.catalog option" },
                { new String[] { "--source.driver=org.postgresql.Driver", "--source.url=jdbc:postgresql:test",
                        "--source.catalog=catalog" }, "PostgreSQL doesn't support source.catalog option" } };
    }

    @Test(dataProvider = "sourceGroupValidation", expectedExceptions = OptionException.class)
    public void testSourceGroupValidation(String[] arguments, String exception) {
        Group group = cliRunSupport.createSourceGroup();
        parser.parse(arguments, group);
        fail(exception);
    }

    @DataProvider(name = "targetGroup")
    public Object[][] createTargetGroupData() {
        String[] arguments = { "--target.url=jdbc:com.nuodb://localhost/test", "--target.username=dba",
                "--target.password=goalie", "--target.schema=hockey" };

        DriverConnectionSpec expected = new DriverConnectionSpec();
        expected.setDriver(JdbcConstants.NUODB_DRIVER);
        expected.setUrl("jdbc:com.nuodb://localhost/test");
        expected.setUsername("dba");
        expected.setPassword("goalie");
        expected.setSchema("hockey");
        return new Object[][] { { arguments, expected } };
    }

    @Test(dataProvider = "targetGroup")
    public void testTargetGroup(String[] arguments, DriverConnectionSpec expected) {
        Group group = cliRunSupport.createTargetGroup();
        assertNotNull(group, "Target group of options is required");

        OptionSet options = parser.parse(arguments, group);

        DriverConnectionSpec actual = cliRunSupport.parseTargetGroup(options, group);
        assertNotNull(actual, "Connection specification is expected");
        assertEquals(actual, expected);
    }

    @DataProvider(name = "inputGroup")
    public Object[][] createInputGroupData() {
        String[] arguments = { "--input.path=/tmp/dump.cat", "--input.csv.encoding=utf-8",
                "--input.csv.delimiter=tab" };

        ResourceSpec expected = new ResourceSpec();
        expected.setPath("/tmp/dump.cat");

        Map<String, Object> attributes = Maps.newHashMap();
        attributes.put("csv.encoding", "utf-8");
        attributes.put("csv.delimiter", "tab");
        expected.setAttributes(attributes);
        return new Object[][] { { arguments, expected } };
    }

    @Test(dataProvider = "inputGroup")
    public void testInputGroup(String[] arguments, ResourceSpec expected) {
        Group group = cliRunSupport.createInputGroup();
        assertNotNull(group, "Input group of options is required");

        OptionSet options = parser.parse(arguments, group);

        ResourceSpec actual = cliRunSupport.parseInputGroup(options, group);
        assertNotNull(actual, "Resource specification is expected");
        assertEquals(actual, expected);
    }

    @DataProvider(name = "outputGroup")
    public Object[][] createOutputGroupData() {
        String[] arguments = { "--output.path=/tmp/dump.cat", "--output.type=bson" };

        ResourceSpec expected = new ResourceSpec();
        expected.setPath("/tmp/dump.cat");
        expected.setType("bson");
        return new Object[][] { { arguments, expected } };
    }

    @Test(dataProvider = "outputGroup")
    public void testOutputGroup(String[] arguments, ResourceSpec expected) {
        Group group = cliRunSupport.createOutputGroup();
        assertNotNull(group, "Output group of options is required");

        OptionSet options = parser.parse(arguments, group);

        ResourceSpec actual = cliRunSupport.parseOutputGroup(options, group);
        assertNotNull(actual, "Resource specification is expected");
        assertEquals(actual, expected);
    }

    @DataProvider(name = "timeZone")
    public Object[][] createTimeZoneData() {
        return new Object[][] { { new String[] { "-z=UTC" }, getTimeZone("UTC") },
                { new String[] { "--time.zone=EST" }, getTimeZone("EST") },
                { new String[] { "--time.zone=GMT+2" }, getTimeZone("GMT+2") } };
    }

    @Test(dataProvider = "timeZone")
    public void testTimeZone(String[] arguments, TimeZone expected) {
        Option option = cliRunSupport.createTimeZoneOption();
        assertNotNull(option, "Time zone option is required");

        OptionSet options = parser.parse(arguments, option);

        TimeZone actual = cliRunSupport.parseTimeZoneOption(options, option);
        assertNotNull(actual, "Time zone is expected");
        assertEquals(actual, expected);
    }

    @DataProvider(name = "tableGroup")
    public Object[][] createTableGroupData() {
        Collection<Object[]> data = newArrayList();

        // #1 verify inclusion switches first
        String[] arguments1 = { "--table=table1,*pattern1*,test.*.fields" };
        Map<Table, Boolean> tables1 = newHashMap();
        // verify exact matching
        tables1.put(createTable("catalog1", "schema1", "table1"), true);
        tables1.put(createTable("catalog1", "schema1", "table2"), false);
        // verify pattern
        tables1.put(createTable("catalog1", "schema1", "pattern1_"), true);
        tables1.put(createTable("catalog1", "schema1", "_pattern1_xyz"), true);
        tables1.put(createTable("catalog1", "schema1", "pattern2"), false);
        // match wild-carded schema name
        tables1.put(createTable("test", "user1", "fields"), true);
        tables1.put(createTable("test", "user2", "fields"), true);
        data.add(new Object[] { arguments1, tables1 });

        // #2 verify exclusion switches next
        String[] arguments2 = { "--table.exclude=exclude_table1,*exclude_pattern1*,catalog3.schema.*" };
        Map<Table, Boolean> tables2 = newHashMap();
        tables2.put(createTable("catalog1", "schema1", "exclude_table1"), false);
        tables2.put(createTable("catalog2", "schema2", "exclude_pattern1_xyz"), false);
        // any table from catalog3.schema is excluded
        tables2.put(createTable("catalog3", "schema", "exclude_pattern1_xyz"), false);
        // this one is from different schema
        tables2.put(createTable("catalog3", "schema2", "table"), true);
        data.add(new Object[] { arguments2, tables2 });

        return data.toArray(new Object[data.size()][]);
    }

    /**
     * Verifies that --table=table1,pattern1 and --table.exclude=table1,pattern1
     * command line switches are parsed properly and constructed meta data
     * filter manager accept/does not accept tables from tables map according to
     * command line switches.
     *
     * @param arguments
     *            --table=table1,*pattern1* or --table.exclude=exclude.table1,
     *            *exclude.pattern1* switches or both.
     * @param tables
     *            table to boolean map, where boolean determines whether the
     *            table should accepted by meta data filter manager.
     */
    @Test(dataProvider = "tableGroup")
    public void testTableGroup(String[] arguments, Map<Table, Boolean> tables) {
        Option group = cliRunSupport.createMetaDataFilterManagerGroup();
        assertNotNull(group, "Table group is required");

        OptionSet options = parser.parse(arguments, group);
        MetaDataFilterManager filterManager = cliRunSupport.parseMetaDataFilterManagerGroup(options, group);
        MetaDataFilter<Table> filter = filterManager.getMetaDataFilter(MetaDataType.TABLE);
        for (Map.Entry<Table, Boolean> entry : tables.entrySet()) {
            Table table = entry.getKey();
            boolean accepts = entry.getValue();
            assertEquals(filter.accepts(table), accepts,
                    MessageFormat.format("Table {0} should be {1,choice,0#not accepted|1#accepted by filter manager}",
                            table.getQualifiedName(), accepts ? 1 : 0));
        }
    }
}
