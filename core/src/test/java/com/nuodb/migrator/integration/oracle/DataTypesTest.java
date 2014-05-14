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
package com.nuodb.migrator.integration.oracle;

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
@Test(groups = { "oracleintegrationtest" }, dependsOnGroups = { "dataloadperformed" })
public class DataTypesTest extends MigrationTestBase {

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_number() throws Exception {
		String sqlStr1 = "select * from testdata_number";
		String sqlStr2 = "select * from \"testdata_number\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_integer() throws Exception {
		String sqlStr1 = "select * from testdata_integer";
		String sqlStr2 = "select * from \"testdata_integer\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_binary_float() throws Exception {
		String sqlStr1 = "select * from testdata_binary_float";
		String sqlStr2 = "select * from \"testdata_binary_float\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_binary_double() throws Exception {
		String sqlStr1 = "select * from testdata_binary_double";
		String sqlStr2 = "select * from \"testdata_binary_double\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_float() throws Exception {
		String sqlStr1 = "select * from testdata_float";
		String sqlStr2 = "select * from \"testdata_float\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_real() throws Exception {
		String sqlStr1 = "select * from testdata_real";
		String sqlStr2 = "select * from \"testdata_real\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_varchar() throws Exception {
		String sqlStr1 = "select * from testdata_varchar";
		String sqlStr2 = "select * from \"testdata_varchar\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_char_varying() throws Exception {
		String sqlStr1 = "select * from testdata_char_varying";
		String sqlStr2 = "select * from \"testdata_char_varying\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_char() throws Exception {
		String sqlStr1 = "select * from testdata_char";
		String sqlStr2 = "select * from \"testdata_char\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_date() throws Exception {
		String sqlStr1 = "select * from testdata_date";
		String sqlStr2 = "select * from \"testdata_date\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_timestamp() throws Exception {
		String sqlStr1 = "select * from testdata_timestamp";
		String sqlStr2 = "select * from \"testdata_timestamp\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_withtimezone() throws Exception {
		String sqlStr1 = "select * from testdata_withtimezone";
		String sqlStr2 = "select * from \"testdata_withtimezone\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_withlocaltimezone() throws Exception {
		String sqlStr1 = "select * from testdata_withlocaltimezone";
		String sqlStr2 = "select * from \"testdata_withlocaltimezone\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_clob() throws Exception {
		String sqlStr1 = "select * from testdata_clob";
		String sqlStr2 = "select * from \"testdata_clob\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_blob() throws Exception {
		String sqlStr1 = "select * from testdata_blob";
		String sqlStr2 = "select * from \"testdata_blob\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_raw() throws Exception {
		String sqlStr1 = "select * from testdata_raw";
		String sqlStr2 = "select * from \"testdata_raw\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_long() throws Exception {
		String sqlStr1 = "select * from testdata_long";
		String sqlStr2 = "select * from \"testdata_long\"";
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
