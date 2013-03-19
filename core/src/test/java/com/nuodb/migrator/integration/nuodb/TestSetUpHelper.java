package com.nuodb.migrator.integration.nuodb;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.Assert;

public class TestSetUpHelper {

	public static void setUpBlobs(Connection con) throws SQLException  {
		String sqlStr = "insert into datatypes1 (\"c2\") values ('1')";
		Statement stmt1 = null;
		try {
			stmt1 = con.createStatement();
			int rows = stmt1.executeUpdate(sqlStr);
			Assert.assertEquals(rows, 1);
			// second time should fail
			try {
				rows = stmt1.executeUpdate(sqlStr);
			} catch (SQLException e) {
				Assert.assertTrue(e.getMessage().contains("datatypes1_UNIQUE"));
			}
		} finally {
			stmt1.close();
		}
	}
}
