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
package com.nuodb.migrator.integration.sqlserver;

import com.nuodb.migrator.integration.MigrationTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test to make sure all the Tables, Constraints, Views, Triggers etc have been
 * migrated.
 * 
 * @author Krishnamoorthy Dhandapani
 */
@Test(groups = { "sqlserverintegrationtest" }, dependsOnGroups = { "dataloadperformed" })
public class DataTypesTest extends MigrationTestBase {

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_bigint() throws Exception {
		String sqlStr1 = "select * from testdata_bigint";
		String sqlStr2 = "select * from \"testdata_bigint\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_integer() throws Exception {
		String sqlStr1 = "select * from testdata_integer";
		String sqlStr2 = "select * from \"testdata_integer\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_smallint() throws Exception {
		String sqlStr1 = "select * from testdata_smallint";
		String sqlStr2 = "select * from \"testdata_smallint\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_tinyint() throws Exception {
		String sqlStr1 = "select * from testdata_tinyint";
		String sqlStr2 = "select * from \"testdata_tinyint\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_varchar() throws Exception {
		String sqlStr1 = "select * from testdata_varchar";
		String sqlStr2 = "select * from \"testdata_varchar\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_nvarchar() throws Exception {
		String sqlStr1 = "select * from testdata_nvarchar";
		String sqlStr2 = "select * from \"testdata_nvarchar\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_char() throws Exception {
		String sqlStr1 = "select * from testdata_char";
		String sqlStr2 = "select * from \"testdata_char\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_nchar() throws Exception {
		String sqlStr1 = "select * from testdata_nchar";
		String sqlStr2 = "select * from \"testdata_nchar\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_text() throws Exception {
		String sqlStr1 = "select * from testdata_text";
		String sqlStr2 = "select * from \"testdata_text\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_ntext() throws Exception {
		String sqlStr1 = "select * from testdata_ntext";
		String sqlStr2 = "select * from \"testdata_ntext\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_bit() throws Exception {
		String sqlStr1 = "select * from testdata_bit";
		String sqlStr2 = "select * from \"testdata_bit\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_numeric() throws Exception {
		String sqlStr1 = "select * from testdata_numeric";
		String sqlStr2 = "select * from \"testdata_numeric\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_decimal() throws Exception {
		String sqlStr1 = "select * from testdata_decimal";
		String sqlStr2 = "select * from \"testdata_decimal\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_money() throws Exception {
		String sqlStr1 = "select * from testdata_money";
		String sqlStr2 = "select * from \"testdata_money\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_smallmoney() throws Exception {
		String sqlStr1 = "select * from testdata_smallmoney";
		String sqlStr2 = "select * from \"testdata_smallmoney\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_real() throws Exception {
		String sqlStr1 = "select * from testdata_real";
		String sqlStr2 = "select * from \"testdata_real\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_float() throws Exception {
		String sqlStr1 = "select * from testdata_float";
		String sqlStr2 = "select * from \"testdata_float\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_hierarchyid() throws Exception {
		String sqlStr1 = "select * from testdata_hierarchyid";
		String sqlStr2 = "select * from \"testdata_hierarchyid\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_uniqueidentifier() throws Exception {
		String sqlStr1 = "select * from testdata_uniqueidentifier";
		String sqlStr2 = "select * from \"testdata_uniqueidentifier\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_image() throws Exception {
		String sqlStr1 = "select * from testdata_image";
		String sqlStr2 = "select * from \"testdata_image\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_binary() throws Exception {
		String sqlStr1 = "select * from testdata_binary";
		String sqlStr2 = "select * from \"testdata_binary\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_varbinary() throws Exception {
		String sqlStr1 = "select * from testdata_varbinary";
		String sqlStr2 = "select * from \"testdata_varbinary\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_time() throws Exception {
		String sqlStr1 = "select * from testdata_time";
		String sqlStr2 = "select * from \"testdata_time\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_date() throws Exception {
		String sqlStr1 = "select * from testdata_date";
		String sqlStr2 = "select * from \"testdata_date\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_smalldatetime() throws Exception {
		String sqlStr1 = "select * from testdata_smalldatetime";
		String sqlStr2 = "select * from \"testdata_smalldatetime\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_datetime() throws Exception {
		String sqlStr1 = "select * from testdata_datetime";
		String sqlStr2 = "select * from \"testdata_datetime\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_datetimeoffset() throws Exception {
		String sqlStr1 = "select * from testdata_datetimeoffset";
		String sqlStr2 = "select * from \"testdata_datetimeoffset\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_datetime2() throws Exception {
		String sqlStr1 = "select * from testdata_datetime2";
		String sqlStr2 = "select * from \"testdata_datetime2\"";
		verifyData(sqlStr1, sqlStr2);
	}

	private void verifyData(String sqlStr1, String sqlStr2)
			throws SQLException, Exception {
		Statement stmt1 = null, stmt2 = null;
		ResultSet rs1 = null, rs2 = null;
		try {
			stmt1 = sourceConnection.createStatement();
			rs1 = stmt1.executeQuery(sqlStr1);

			Assert.assertNotNull(rs1);

			stmt2 = nuodbConnection.createStatement();
			rs2 = stmt2.executeQuery(sqlStr2);

			Assert.assertNotNull(rs2);

			rsUtil.assertIsEqual(rs1, rs2, true, true);

		} finally {
			closeAll(rs1, stmt1, rs2, stmt2);
		}
	}

}
