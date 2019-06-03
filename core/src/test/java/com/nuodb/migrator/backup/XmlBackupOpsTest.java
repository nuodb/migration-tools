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

import com.nuodb.migrator.Migrator;
import com.nuodb.migrator.backup.format.value.ValueType;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.ForeignKey;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.type.JdbcType;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.nuodb.migrator.backup.format.value.ValueType.STRING;
import static com.nuodb.migrator.jdbc.metadata.Identifier.EMPTY;
import static com.nuodb.migrator.jdbc.type.JdbcTypeOptions.newOptions;
import static org.apache.commons.io.IOUtils.toInputStream;
import static org.testng.Assert.assertEquals;

/**
 * @author Sergey Bushik
 */
public class XmlBackupOpsTest {

    private XmlBackupOps xmlBackupOps;

    @BeforeMethod
    public void setUp() {
        xmlBackupOps = new XmlBackupOps();
    }

    @Test
    public void testReadRowSet() {
        Backup expected = new Backup();
        expected.setDatabase(new Database());
        expected.setFormat("csv");

        TableRowSet rowSet = new TableRowSet();
        rowSet.setType("table");
        rowSet.setName("test.t1");
        rowSet.setRowCount(1L);
        rowSet.setCatalog("test");
        rowSet.setTable("t1");
        rowSet.addColumn("f1", STRING);

        Chunk chunk = new Chunk();
        chunk.setName("test.t1.csv");
        chunk.setRowCount(1L);
        rowSet.addChunk(chunk);
        expected.addRowSet(rowSet);

        String input = "<?xml version=\"1.0\"?>\n" + "<backup version=\"" + Migrator.getVersion()
                + "\" format=\"csv\">\n" + "  <database/>\n"
                + "  <row-set type=\"table\" name=\"test.t1\" row-count=\"1\" catalog=\"test\" table=\"t1\">\n"
                + "    <column name=\"f1\" value-type=\"string\"/>\n"
                + "    <chunk name=\"test.t1.csv\" row-count=\"1\"/>\n" + "  </row-set>\n" + "</backup>";
        Backup actual = xmlBackupOps.read(toInputStream(input));
        assertEquals(actual, expected);
    }

    /**
     * Tests MIG-44 implicitly declared tables (referenced by foreign-key)
     */
    @Test
    public void testReadExplicitlyDeclaredTables() {
        Backup expected = new Backup();
        Database database = new Database();
        Schema schema = database.addCatalog("test").addSchema(EMPTY);
        Table foreignTable = schema.addTable("t1");
        Column foreignColumn = foreignTable.addColumn("t1_f1");
        foreignColumn.setJdbcType(new JdbcType(new JdbcTypeDesc(4, "INT"), newOptions(10, 10, 0)));
        ForeignKey foreignKey = new ForeignKey("fk_1");
        Table primaryTable = schema.addTable("t2");
        Column primaryColumn = primaryTable.addColumn("t2_f1");
        foreignKey.addReference(primaryColumn, foreignColumn);
        foreignTable.addForeignKey(foreignKey);
        expected.setFormat("csv");
        expected.setDatabase(database);

        String input = "<?xml version=\"1.0\"?>\n" + "<backup version=\"" + Migrator.getVersion()
                + "\" format=\"csv\">\n" + "  <database>\n" + "    <catalog name=\"test\">\n" + "      <schema>\n"
                + "        <table name=\"t1\" type=\"TABLE\">\n" + "          <column name=\"t1_f1\">\n"
                + "            <type code=\"4\" name=\"INT\" size=\"10\" precision=\"10\" scale=\"0\"/>\n"
                + "          </column>\n"
                + "          <foreign-key name=\"fk_1\" primary-catalog=\"test\" primary-table=\"t2\" "
                + "foreign-catalog=\"test\" foreign-table=\"t1\" update-action=\"no_action\" "
                + "delete-action=\"no_action\" deferrability=\"not_deferrable\">\n"
                + "            <reference primary-column=\"t2_f1\" foreign-column=\"t1_f1\"/>\n"
                + "          </foreign-key>\n" + "        </table>\n" + "      </schema>\n" + "    </catalog>\n"
                + "  </database>\n" + "</backup>";
        Backup actual = xmlBackupOps.read(toInputStream(input));
        assertEquals(actual, expected);
        assertEquals(actual.getDatabase().getSchemas(), expected.getDatabase().getSchemas());
    }
}
