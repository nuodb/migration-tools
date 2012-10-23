package com.nuodb.tools.migration.dump;


import com.nuodb.tools.migration.dump.query.Query;
import com.nuodb.tools.migration.dump.query.SelectQuery;
import com.nuodb.tools.migration.jdbc.JdbcServicesImpl;
import com.nuodb.tools.migration.jdbc.connection.ConnectionProvider;
import com.nuodb.tools.migration.jdbc.metamodel.*;
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

public class DumpWriterIntegrationTest {


    private DumpWriter dumpWriter;

    @Before
    public void setUp() throws Exception {
        dumpWriter = new DumpWriter();

    }

    @Test
    public void testCreateQueries() throws Exception {
        final Database database = new Database();
        database.setCatalogs(new HashMap<Name, Catalog>(){{
            final Catalog testCatalog = new Catalog(database, TEST_CATALOG_NAME);
            final Schema newSchema = testCatalog.getSchema(TEST_SCHEMA_NAME);
            final Table testTable = newSchema.createTable(TEST_TABLE_NAME, "");
            testTable.createColumn("FIRST");
            testTable.createColumn("SECOND");
            testTable.createColumn("THIRD");

            put(Name.valueOf("TEST_CATALOG"), testCatalog);
        }});
        final List<Query> queries =
                dumpWriter.createQueries(database,
                        new ArrayList<SelectQuerySpec>(),
                        new ArrayList<NativeQuerySpec>());


        Assert.assertFalse(queries.isEmpty());
        Assert.assertEquals(1, queries.size());
        Assert.assertTrue(queries.get(0) instanceof SelectQuery);
    }

    @Test
    public void testIntrospect() throws Exception {
        final DriverManagerConnectionSpec connectionSpec = createTestNuoDBConnectionSpec();
        final JdbcServicesImpl jdbcServices = new JdbcServicesImpl(connectionSpec);
        ConnectionProvider connectionProvider = jdbcServices.getConnectionProvider();
        Connection connection = connectionProvider.getConnection();

        final Database introspect = dumpWriter.introspect(jdbcServices, connection);
        Assert.assertNotNull(introspect);
    }

    @Test
    public void testDump() throws Exception {
        final DriverManagerConnectionSpec connectionSpec = createTestNuoDBConnectionSpec();
        final JdbcServicesImpl jdbcServices = new JdbcServicesImpl(connectionSpec);
        ConnectionProvider connectionProvider = jdbcServices.getConnectionProvider();
        Connection connection = connectionProvider.getConnection();

        final SelectQuery selectQuery = createTestSelectQuery();

        dumpWriter.dump(connection, selectQuery, getDefaultOutputFormat());
    }
}
