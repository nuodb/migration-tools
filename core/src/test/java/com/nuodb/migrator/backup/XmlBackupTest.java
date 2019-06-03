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
package com.nuodb.migrator.backup;

import com.google.common.collect.Lists;
import com.nuodb.migrator.jdbc.metadata.Catalog;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.DriverInfo;
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.UserDefinedType;
import com.nuodb.migrator.jdbc.type.JdbcEnumType;
import com.nuodb.migrator.jdbc.type.JdbcType;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;
import com.nuodb.migrator.spec.DriverConnectionSpec;
import com.nuodb.migrator.utils.Equality;
import com.nuodb.migrator.utils.xml.XmlPersister;
import org.simpleframework.xml.stream.Format;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.StringReader;
import java.io.StringWriter;

import static com.nuodb.migrator.Migrator.getVersion;
import static com.nuodb.migrator.backup.format.csv.CsvFormat.TYPE;
import static com.nuodb.migrator.jdbc.type.JdbcTypeOptions.newOptions;
import static com.nuodb.migrator.utils.Equalities.defaultEquality;
import static com.nuodb.migrator.utils.Equalities.reflectionEquality;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("all")
public class XmlBackupTest {

    private XmlPersister xmlPersister;

    @BeforeMethod
    public void setUp() {
        xmlPersister = new XmlBackupOps() {
            @Override
            protected Format createFormat() {
                return new Format(0);
            }
        }.getXmlPersister();
    }

    @DataProvider(name = "read")
    public Object[][] createReadData() {
        // backup read
        Backup backup = new Backup();
        backup.setFormat("csv");
        backup.setVersion(getVersion());
        backup.setDatabase(new Database());
        backup.setRowSets(Lists.<RowSet>newArrayList(new TableRowSet()));

        // database read
        Database database = new Database();
        database.setDriverInfo(new DriverInfo());
        database.setDatabaseInfo(new DatabaseInfo());
        database.setConnectionSpec(new DriverConnectionSpec());
        database.addCatalog("catalog1");
        database.addCatalog("catalog2");

        // driver info read
        DriverInfo driverInfo = new DriverInfo();
        driverInfo.setName("MySQL-AB JDBC Driver");
        driverInfo.setVersion("mysql-connector-java-5.1.20");
        driverInfo.setMajorVersion(5);
        driverInfo.setMinorVersion(1);

        // database info read
        DatabaseInfo databaseInfo = new DatabaseInfo();
        databaseInfo.setProductName("MySQL");
        databaseInfo.setProductVersion("5.5.31-log");
        databaseInfo.setMajorVersion(5);
        databaseInfo.setMinorVersion(5);

        // connection spec read
        DriverConnectionSpec connectionSpec = new DriverConnectionSpec();
        connectionSpec.setDriver("com.mysql.jdbc.Driver");
        connectionSpec.setUrl("jdbc:mysql://localhost/test");
        connectionSpec.setCatalog("test");
        connectionSpec.setUsername("root");

        // catalog read
        Catalog catalog = new Catalog("catalog1");
        catalog.addSchema("schema1");
        catalog.addSchema("schema2");

        // schema read
        Schema schema = new Schema("schema1");
        Sequence sequence = new Sequence("sequence1");
        sequence.setStartWith(6);
        sequence.setLastValue(6);
        sequence.setIncrementBy(3);
        sequence.setMinValue(0);
        sequence.setMaxValue(2147483647);
        sequence.setCache(50);
        sequence.setCycle(false);
        sequence.setOrder(true);
        sequence.setTemporary(false);
        schema.addSequence(sequence);

        UserDefinedType userDefinedType = new UserDefinedType();
        userDefinedType.setName("MTYPE");
        userDefinedType.setCode("ARRAY");
        schema.addUserDefinedType(userDefinedType);

        // table read
        Table table = new Table("table1");
        Column column = table.addColumn("column1");
        column.setNullable(true);
        column.setJdbcType(new JdbcType(new JdbcTypeDesc(12, "VARCHAR"), newOptions(20, 20, 0)));
        schema.addTable(table);

        // column read
        column = new Column("column1");
        column.setNullable(true);
        JdbcEnumType jdbcType = new JdbcEnumType(new JdbcTypeDesc(1, "ENUM"), newOptions(1, 1, 0));
        jdbcType.addValue("a");
        jdbcType.addValue("b");
        jdbcType.addValue("c");
        column.setJdbcType(jdbcType);
        return new Object[][] {
                { "<backup version=\"" + getVersion() + "\" format=\"csv\">\n" + "<database/>\n" + "<row-set/>\n"
                        + "</backup>", backup, defaultEquality() },
                { "<database>\n" + "<driver-info/>\n" + "<database-info/>\n" + "<connection-spec/>\n"
                        + "<catalog name=\"catalog1\"/>\n" + "<catalog name=\"catalog2\"/>\n" + "</database>", database,
                        defaultEquality() },
                { "<driver-info>\n" + "<name>MySQL-AB JDBC Driver</name>\n"
                        + "<version>mysql-connector-java-5.1.20</version>\n" + "<major-version>5</major-version>\n"
                        + "<minor-version>1</minor-version>\n" + "</driver-info>", driverInfo, defaultEquality() },
                { "<database-info>\n" + "<product-name>MySQL</product-name>\n"
                        + "<product-version>5.5.31-log</product-version>\n" + "<major-version>5</major-version>\n"
                        + "<minor-version>5</minor-version>\n" + "</database-info>", databaseInfo, defaultEquality() },
                { "<connection-spec type=\"driver\" catalog=\"test\">\n" + "<driver>com.mysql.jdbc.Driver</driver>\n"
                        + "<url>jdbc:mysql://localhost/test</url>\n" + "<username>root</username>\n"
                        + "</connection-spec>", connectionSpec, defaultEquality() },
                { "<catalog name=\"catalog1\">\n" + "<schema name=\"schema1\"/>\n" + "<schema name=\"schema2\"/>\n"
                        + "</catalog>", catalog, reflectionEquality() },
                { "<schema name=\"schema1\">\n" + "<sequence name=\"sequence1\" start-with=\"6\" last-value=\"6\" "
                        + "increment-by=\"3\" min-value=\"0\" max-value=\"2147483647\" "
                        + "cycle=\"false\" order=\"true\" temporary=\"false\" cache=\"50\"/>\n"
                        + "<user-defined-type name=\"MTYPE\" code=\"ARRAY\"/>"
                        + "<table name=\"table1\" type=\"TABLE\">" + "<column name=\"column1\" nullable=\"true\">\n"
                        + "<type code=\"12\" name=\"VARCHAR\" size=\"20\" precision=\"20\" scale=\"0\"/>\n"
                        + "</column>\n" + "</table>\n" + "</schema>", schema, reflectionEquality() },
                { "<column name=\"column1\" nullable=\"true\">\n"
                        + "<enum code=\"1\" name=\"ENUM\" size=\"1\" precision=\"1\" scale=\"0\">\n"
                        + "<value>a</value>\n" + "<value>b</value>\n" + "<value>c</value>\n" + "</enum>\n"
                        + "</column>", column, defaultEquality() } };
    }

    @Test(dataProvider = "read")
    public <T> void testRead(String xml, T expected, Equality<T> equality) {
        T actual = xmlPersister.read((Class<T>) expected.getClass(), new StringReader(xml));
        assertTrue(equality.equals(expected, actual), format("Actual object does not match expected for xml\n%s", xml));
    }

    @DataProvider(name = "write")
    public Object[][] createWriteData() {
        Backup backup = new Backup(TYPE);
        Database database = new Database();
        DatabaseInfo databaseInfo = new DatabaseInfo("NuoDB", null, 1, 29);
        DriverInfo driverInfo = new DriverInfo("NuoDB JDBC Driver", "1.0", 1, 0);

        Catalog catalog = new Catalog("catalog1");
        catalog.addSchema("schema1");
        catalog.addSchema("schema2");

        Schema schema = new Schema("schema1");
        schema.addSequence(new Sequence("sequence1"));
        UserDefinedType userDefinedType = new UserDefinedType();
        userDefinedType.setName("MTYPE");
        userDefinedType.setCode("ARRAY");
        schema.addUserDefinedType(userDefinedType);
        schema.addTable("table1");

        Table table = new Table("table1");
        Column column = new Column("column1");
        JdbcEnumType jdbcType = new JdbcEnumType(new JdbcTypeDesc(1, "ENUM"), newOptions(1, 1, 0));
        jdbcType.addValue("1");
        jdbcType.addValue("2");
        column.setJdbcType(jdbcType);
        table.addColumn(column);
        Index index = new Index();
        index.addColumn(column, 1);
        table.addIndex(index);
        return new Object[][] {
                { backup, "<backup version=\"" + getVersion() + "\" format=\"csv\"><database/></backup>" },
                { database, "<database/>" },
                { databaseInfo,
                        "<databaseInfo>" + "<product-name>NuoDB</product-name>" + "<product-version/>"
                                + "<major-version>1</major-version>" + "<minor-version>29</minor-version>"
                                + "</databaseInfo>" },
                { driverInfo, "<driverInfo>" + "<name>NuoDB JDBC Driver</name>" + "<version>1.0</version>"
                        + "<major-version>0</major-version>" + "<minor-version>1</minor-version>" + "</driverInfo>" },
                { catalog,
                        "<catalog name=\"catalog1\">" + "<schema name=\"schema1\"/>" + "<schema name=\"schema2\"/>"
                                + "</catalog>" },
                { schema,
                        "<schema name=\"schema1\">" + "<sequence name=\"sequence1\"/>"
                                + "<user-defined-type name=\"MTYPE\" code=\"ARRAY\"/>"
                                + "<table name=\"table1\" type=\"TABLE\"/>" + "</schema>" },
                { table, "<table name=\"table1\" type=\"TABLE\">" + "<column name=\"column1\">"
                        + "<enum code=\"1\" name=\"ENUM\" size=\"1\" precision=\"1\" scale=\"0\">" + "<value>1</value>"
                        + "<value>2</value>" + "</enum>" + "</column>" + "<index unique=\"false\">"
                        + "<column name=\"column1\"/>" + "</index>" + "</table>" } };
    }

    @Test(dataProvider = "write")
    public <T> void testWrite(T source, String expected) {
        StringWriter writer = new StringWriter();
        xmlPersister.write(source, writer);
        assertEquals(writer.toString(), expected);
    }
}
