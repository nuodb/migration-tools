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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.nuodb.migrator.jdbc.metadata.Identifier;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilter;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataNameEqualsFilter;
import com.nuodb.migrator.jdbc.metadata.filter.MetaDataNameMatchesFilter;

import org.junit.Assert;
import org.slf4j.Logger;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilters.newAllOfFilters;
import static com.nuodb.migrator.jdbc.metadata.filter.MetaDataFilters.newEitherOfFilters;
import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static org.mockito.Mockito.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyString;

/**
 * @author Mukund
 */
@SuppressWarnings("all")
public class BackupOpsValidateTableFilterTest {

    @DataProvider(name = "getData")
    public Object[][] createGetTypeNameData() throws Exception {
        ArrayList<String> databaseTable = new ArrayList();
        databaseTable.add("test");
        databaseTable.add("customers");

        return new Object[][] { { databaseTable, "temp" }, { databaseTable, "student" } };
    }

    @Test(dataProvider = "getData")
    public void testValidateTableNameFilter(ArrayList<String> databaseTable, String FilterTable) {
        MetaDataNameEqualsFilter mdnef = new MetaDataNameEqualsFilter(MetaDataType.TABLE, true, FilterTable);
        Collection<MetaDataFilter<Table>> filters = newArrayList();
        filters.add(mdnef);
        MetaDataFilter<Table> metaDataTablesFilter = newEitherOfFilters(MetaDataType.TABLE, filters);
        MetaDataFilter<Table> all = newAllOfFilters(MetaDataType.TABLE, metaDataTablesFilter);

        XmlBackupOps backupOps1 = spy(new XmlBackupOps());
        backupOps1.verifyFilter(databaseTable, all);
        verify(backupOps1, times(1)).logWarnMessage(valueOf(anyString()));
    }

    @DataProvider(name = "getData1")
    public Object[][] createGetTypeNameData1() throws Exception {
        ArrayList<String> databaseTable = new ArrayList();
        databaseTable.add("customers");
        databaseTable.add("student");
        return new Object[][] { { databaseTable, "*est" }, { databaseTable, "*emp" } };
    }

    @Test(dataProvider = "getData1")
    public void testValidateTableRegexFilter1(ArrayList<String> databaseTable, String FilterTable) {
        MetaDataNameMatchesFilter mdnmf = new MetaDataNameMatchesFilter(MetaDataType.TABLE, true, FilterTable);
        Collection<MetaDataFilter<Table>> filters = newArrayList();
        filters.add(mdnmf);
        MetaDataFilter<Table> metaDataTablesFilter = newEitherOfFilters(MetaDataType.TABLE, filters);
        MetaDataFilter<Table> all = newAllOfFilters(MetaDataType.TABLE, metaDataTablesFilter);

        XmlBackupOps backupOps1 = spy(new XmlBackupOps());
        backupOps1.verifyFilter(databaseTable, all);
        verify(backupOps1, times(1)).logWarnMessage(valueOf(anyString()));
    }
}
