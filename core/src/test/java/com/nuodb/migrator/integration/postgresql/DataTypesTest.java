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
package com.nuodb.migrator.integration.postgresql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.nuodb.migrator.integration.MigrationTestBase;

/**
 * Test to make sure all the Tables, Constraints, Views, Triggers etc have been
 * migrated.
 * 
 * @author Krishnamoorthy Dhandapani
 */
@Test(groups = { "postgresqlintegrationtest" }, dependsOnGroups = { "dataloadperformed" })
public class DataTypesTest extends MigrationTestBase {

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_tinyint() throws Exception {
		String sqlStr1 = "select * from testdata_smallint";
		String sqlStr2 = "select * from \"testdata_smallint\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_integer() throws Exception {
		String sqlStr1 = "select * from testdata_integer";
		String sqlStr2 = "select * from \"testdata_integer\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_bigint() throws Exception {
		String sqlStr1 = "select * from testdata_bigint";
		String sqlStr2 = "select * from \"testdata_bigint\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_real() throws Exception {
		String sqlStr1 = "select * from testdata_real";
		String sqlStr2 = "select * from \"testdata_real\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_doubleprecision() throws Exception {
		String sqlStr1 = "select * from testdata_doubleprecision";
		String sqlStr2 = "select * from \"testdata_doubleprecision\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_serial() throws Exception {
		String sqlStr1 = "select * from testdata_serial";
		String sqlStr2 = "select * from \"testdata_serial\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_smallserial() throws Exception {
		String sqlStr1 = "select * from testdata_smallserial";
		String sqlStr2 = "select * from \"testdata_smallserial\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_bigserial() throws Exception {
		String sqlStr1 = "select * from testdata_bigserial";
		String sqlStr2 = "select * from \"testdata_bigserial\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_char() throws Exception {
		String sqlStr1 = "select * from testdata_char";
		String sqlStr2 = "select * from \"testdata_char\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_character() throws Exception {
		String sqlStr1 = "select * from testdata_character";
		String sqlStr2 = "select * from \"testdata_character\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_charactervarying() throws Exception {
		String sqlStr1 = "select * from testdata_charactervarying";
		String sqlStr2 = "select * from \"testdata_charactervarying\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_varchar() throws Exception {
		String sqlStr1 = "select * from testdata_varchar";
		String sqlStr2 = "select * from \"testdata_varchar\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_text() throws Exception {
		String sqlStr1 = "select * from testdata_text";
		String sqlStr2 = "select * from \"testdata_text\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_bytea() throws Exception {
		String sqlStr1 = "select * from testdata_bytea";
		String sqlStr2 = "select * from \"testdata_bytea\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_boolean() throws Exception {
		String sqlStr1 = "select * from testdata_boolean";
		String sqlStr2 = "select * from \"testdata_boolean\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_timewithtimezone() throws Exception {
		String sqlStr1 = "select * from testdata_timewithtimezone";
		String sqlStr2 = "select * from \"testdata_timewithtimezone\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_timewithouttimezone() throws Exception {
		String sqlStr1 = "select * from testdata_timewithouttimezone";
		String sqlStr2 = "select * from \"testdata_timewithouttimezone\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_timestampwithtimezone() throws Exception {
		String sqlStr1 = "select * from testdata_timestampwithtimezone";
		String sqlStr2 = "select * from \"testdata_timestampwithtimezone\"";
		verifyData(sqlStr1, sqlStr2);
	}

	@Test(dependsOnGroups = { "dataloadperformed" })
	public void testdata_timestampwithouttimezone() throws Exception {
		String sqlStr1 = "select * from testdata_timestampwithouttimezone";
		String sqlStr2 = "select * from \"testdata_timestampwithouttimezone\"";
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
