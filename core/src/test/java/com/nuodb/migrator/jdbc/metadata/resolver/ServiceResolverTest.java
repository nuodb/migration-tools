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
package com.nuodb.migrator.jdbc.metadata.resolver;

import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class ServiceResolverTest {

    private ServiceResolver<Service> serviceResolver;

    @BeforeMethod
    public void setUp() {
        serviceResolver = spy(new SimpleServiceResolver<Service>());

        serviceResolver.register(new DatabaseInfo("NuoDB"), new ServiceInstance("service-1"));
        serviceResolver.register(new DatabaseInfo("NuoDB", "1.0.1-128"), new ServiceInstance("service-2"));
        serviceResolver.register(new DatabaseInfo("NuoDB", "1.0.1-128", 17), new ServiceInstance("service-3"));
        serviceResolver.register(new DatabaseInfo("NuoDB", "1.0.1-128", 17, 1), new ServiceInstance("service-4"));

        serviceResolver.register(new DatabaseInfo("MySQL"), new ServiceInstance("service-5"));
        serviceResolver.register(new DatabaseInfo("MySQL", "5.5.28"), new ServiceInstance("service-6"));
        serviceResolver.register(new DatabaseInfo("MySQL", "5.5.28", 5), new ServiceInstance("service-7"));
        serviceResolver.register(new DatabaseInfo("MySQL", "5.5.28", 5, 5), new ServiceInstance("service-8"));

        serviceResolver.register(new DatabaseInfo("PostgreSQL"), Service1.class);
        serviceResolver.register(new DatabaseInfo("PostgreSQL", "9.2.3"), Service2.class);
        serviceResolver.register(new DatabaseInfo("PostgreSQL", "9.2.3", 2), Service3.class);
        serviceResolver.register(new DatabaseInfo("PostgreSQL", "9.2.3", 2, 9), Service4.class);
    }

    @DataProvider(name = "resolveService")
    public Object[][] createResolveServiceData() {
        return new Object[][] { { "NuoDB", "1.0.1-129", 18, 2, 18, new ServiceInstance("service-1") },
                { "NuoDB", "1.0.1-128", 16, 0, 16, new ServiceInstance("service-2") },
                { "NuoDB", "1.0.1-128", 17, 0, 17, new ServiceInstance("service-3") },
                { "NuoDB", "1.0.1-128", 17, 2, 17, new ServiceInstance("service-4") }, };
    }

    @Test(dataProvider = "resolveService")
    public void testResolveService(String productName, String productVersion, int majorVersion, int minorVersion,
            int platformVersion, Service service) throws Exception {
        DatabaseMetaData metaData = createDatabaseMetaData(productName, productVersion, majorVersion, minorVersion);
        Connection connection = mock(Connection.class);
        given(connection.getMetaData()).willReturn(metaData);
        given(metaData.getConnection()).willReturn(connection);

        Statement statement = mock(Statement.class);
        given(connection.createStatement()).willReturn(statement);
        ResultSet resultSet = mock(ResultSet.class);
        given(statement.executeQuery(anyString())).willReturn(resultSet);
        given(resultSet.next()).willReturn(true);
        given(resultSet.getInt(1)).willReturn(platformVersion);

        assertEquals(serviceResolver.resolve(connection), service);

        verify(serviceResolver).resolve(eq(metaData));
        verify(serviceResolver).resolve(any(DatabaseInfo.class));
    }

    @DataProvider(name = "resolveServiceClass")
    public Object[][] createResolveServiceClassData() {
        return new Object[][] { { "PostgreSQL", "9.2.4", 3, 10, Service1.class },
                { "PostgreSQL", "9.2.3", 1, 10, Service2.class }, { "PostgreSQL", "9.2.3", 2, 0, Service3.class },
                { "PostgreSQL", "9.2.3", 2, 9, Service4.class }, };
    }

    @Test(dataProvider = "resolveServiceClass")
    public void testResolveServiceClass(String productName, String productVersion, int majorVersion, int minorVersion,
            Class<? extends Service> serviceClass) throws Exception {
        DatabaseMetaData metaData = createDatabaseMetaData(productName, productVersion, majorVersion, minorVersion);
        Connection connection = mock(Connection.class);
        given(connection.getMetaData()).willReturn(metaData);

        Service service = serviceResolver.resolve(connection);
        assertNotNull(service);
        assertEquals(service.getClass(), serviceClass);

        verify(serviceResolver).resolve(eq(metaData));
        verify(serviceResolver).resolve(any(DatabaseInfo.class));
    }

    public static DatabaseMetaData createDatabaseMetaData(String productName, String productVersion, int majorVersion,
            int minorVersion) throws Exception {
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        given(metaData.getDatabaseProductName()).willReturn(productName);
        given(metaData.getDatabaseProductVersion()).willReturn(productVersion);
        given(metaData.getDatabaseMajorVersion()).willReturn(majorVersion);
        given(metaData.getDatabaseMinorVersion()).willReturn(minorVersion);
        return metaData;
    }
}
