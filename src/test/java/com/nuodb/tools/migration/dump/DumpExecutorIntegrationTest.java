package com.nuodb.tools.migration.dump;


import com.nuodb.tools.migration.jdbc.JdbcServices;
import com.nuodb.tools.migration.jdbc.JdbcServicesImpl;
import com.nuodb.tools.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.tools.migration.jdbc.metamodel.*;
import com.nuodb.tools.migration.jdbc.query.Query;
import com.nuodb.tools.migration.jdbc.query.SelectQuery;
import com.nuodb.tools.migration.spec.DriverManagerConnectionSpec;
import com.nuodb.tools.migration.spec.NativeQuerySpec;
import com.nuodb.tools.migration.spec.SelectQuerySpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.nuodb.tools.migration.TestConstants.*;

public class DumpExecutorIntegrationTest {

    private DumpExecutor dumpExecutor;

    @Before
    public void setUp() throws Exception {
        dumpExecutor = new DumpExecutor();
    }

    @Test
    public void testCreateQueries() throws Exception {
        final Database database = new Database();
        database.setCatalogs(new HashMap<Name, Catalog>() {{
            final Catalog testCatalog = new Catalog(database, TEST_CATALOG_NAME);
            final Schema newSchema = testCatalog.getSchema(TEST_SCHEMA_NAME);
            final Table testTable = newSchema.createTable(TEST_TABLE_NAME, "");
            testTable.createColumn("FIRST");
            testTable.createColumn("SECOND");
            testTable.createColumn("THIRD");
            put(Name.valueOf("TEST_CATALOG"), testCatalog);
        }});
        final List<Query> queries =
                dumpExecutor.createQueries(database,
                        new ArrayList<SelectQuerySpec>(),
                        new ArrayList<NativeQuerySpec>());

        Assert.assertFalse(queries.isEmpty());
        Assert.assertEquals(1, queries.size());
        Assert.assertTrue(queries.get(0) instanceof SelectQuery);
    }

    @Test
    public void testIntrospect() throws Exception {
        final DriverManagerConnectionSpec connectionSpec = createTestNuoDBConnectionSpec();
        final JdbcServices jdbcServices = new JdbcServicesImpl(connectionSpec);
        final ConnectionProvider connectionProvider = jdbcServices.getConnectionProvider();
        final Connection connection = connectionProvider.getConnection();
        final Database database = dumpExecutor.introspect(jdbcServices, connection);
        Assert.assertNotNull(database);
    }

    @Test
    public void testDump() throws Exception {
        final DriverManagerConnectionSpec connectionSpec = createTestNuoDBConnectionSpec();
        final JdbcServices jdbcServices = new JdbcServicesImpl(connectionSpec);
        final ConnectionProvider connectionProvider = jdbcServices.getConnectionProvider();
        final Connection connection = connectionProvider.getConnection();
        final SelectQuery selectQuery = createTestSelectQuery();
        dumpExecutor.doExecute(connection, selectQuery, getDefaultOutputFormat());
    }
}