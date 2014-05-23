/**
 * Copyright (c) 2014, NuoDB, Inc.
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
import com.nuodb.migrator.jdbc.metadata.Database;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.commons.io.IOUtils.toInputStream;
import static org.testng.Assert.assertEquals;

/**
 * @author Sergey Bushik
 */
public class XmlBackupManagerTest {

    private XmlBackupManager backupManager;

    @BeforeMethod
    public void setUp() {
        backupManager = new XmlBackupManager(".");
    }

    @DataProvider(name = "readBackup")
    public Object[][] createReadBackupData() {
        Backup backup = new Backup();
        backup.setDatabase(new Database());
        backup.setFormat("csv");

        TableRowSet rowSet = new TableRowSet();
        rowSet.setType("table");
        rowSet.setName("test.t1");
        rowSet.setRowCount(1L);
        rowSet.setCatalog("test");
        rowSet.setTable("t1");
        rowSet.addColumn("f1", "string");

        Chunk chunk = new Chunk();
        chunk.setName("test.t1.csv");
        chunk.setRowCount(1L);
        rowSet.addChunk(chunk);
        backup.addRowSet(rowSet);
        return new Object[][]{{
                "<?xml version=\"1.0\"?>\n" +
                "<backup version=\"" + Migrator.getVersion() + "\" format=\"csv\">\n" +
                "  <database/>\n" +
                "  <row-set type=\"table\" name=\"test.t1\" row-count=\"1\" catalog=\"test\" table=\"t1\">\n" +
                "    <column name=\"f1\" value-type=\"string\"/>\n" +
                "    <chunk name=\"test.t1.csv\" row-count=\"1\"/>\n" +
                "  </row-set>\n" +
                "</backup>",
                backup
        }};
    }

    @Test(dataProvider = "readBackup")
    public void testReadBackup(String input, Backup backup) {
        assertEquals(backupManager.readBackup(toInputStream(input)), backup);
    }
}
