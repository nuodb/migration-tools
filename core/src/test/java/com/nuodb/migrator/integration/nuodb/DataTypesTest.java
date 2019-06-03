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
package com.nuodb.migrator.integration.nuodb;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.nuodb.migrator.integration.common.BaseDataTypeTest;

/**
 * @author Krishnamoorthy Dhandapani
 */
@Test(groups = { "nuodbintegrationtest" }, dependsOnGroups = { "dataloadperformed" })
public class DataTypesTest extends BaseDataTypeTest {

    @DataProvider(name = "dataTypeNuodb")
    public Object[][] createDataTypeData() {
        return new Object[][] { { "testdata_smallint" }, { "testdata_bigint" }, { "testdata_integer" },
                { "testdata_decimal" }, { "testdata_double_precision" }, { "testdata_number" }, { "testdata_numeric" },
                { "testdata_string" }, { "testdata_varchar" }, { "testdata_clob" }, { "testdata_national_char" },
                { "testdata_blob" }, { "testdata_binary" }, { "testdata_binary_varying" }, { "testdata_boolean" },
                { "testdata_enum" }, { "testdata_date" }, { "testdata_timestamp" }, { "testdata_time" } };
    }

    @Test(dataProvider = "dataTypeNuodb")
    public void testDataType(String tableName) throws Exception {
        super.testDataType(tableName);
    }
}
