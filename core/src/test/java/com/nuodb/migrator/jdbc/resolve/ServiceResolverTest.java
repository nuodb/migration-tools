/**
 * Copyright (c) 2012, NuoDB, Inc.
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
package com.nuodb.migrator.jdbc.resolve;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class ServiceResolverTest {

    private SimpleServiceResolver<Service> serviceResolver;

    @BeforeMethod(groups = "resolveService")
    public void setUp() {
        serviceResolver = spy(new SimpleServiceResolver<Service>());

        serviceResolver.register(new DatabaseInfo("NuoDB"), new Service("instance-1"));
        serviceResolver.register(new DatabaseInfo("NuoDB", "1.0.1-128"), new Service("instance-2"));
        serviceResolver.register(new DatabaseInfo("NuoDB", "1.0.1-128", 17), new Service("instance-3"));
        serviceResolver.register(new DatabaseInfo("NuoDB", "1.0.1-128", 17, 1), new Service("instance-4"));

        serviceResolver.register(new DatabaseInfo("MySQL"), new Service("instance-4"));
        serviceResolver.register(new DatabaseInfo("MySQL", "5.5.28"), new Service("instance-5"));
        serviceResolver.register(new DatabaseInfo("MySQL", "5.5.28", 5), new Service("instance-6"));
        serviceResolver.register(new DatabaseInfo("MySQL", "5.5.28", 5, 5), new Service("instance-7"));
    }

    @DataProvider(name = "resolveService")
    public Object[][] createResolveServiceData() {
        return new Object[][]{
                {"NuoDB", "1.0.1-129", 18, 5, new Service("instance-1")},
                {"NuoDB", "1.0.1-128", 18, 5, new Service("instance-2")},
                {"NuoDB", "1.0.1-128", 17, 5, new Service("instance-3")},
                {"NuoDB", "1.0.1-128", 17, 1, new Service("instance-4")},
        };
    }

    @Test(dataProvider = "resolveService", groups = "resolveService")
    public void testResolveService(String productName, String productVersion,
                                   int minorVersion, int majorVersion, Service service) throws Exception {
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        given(metaData.getDatabaseProductName()).willReturn(productName);
        given(metaData.getDatabaseProductVersion()).willReturn(productVersion);
        given(metaData.getDatabaseMinorVersion()).willReturn(minorVersion);
        given(metaData.getDatabaseMajorVersion()).willReturn(majorVersion);

        Connection connection = mock(Connection.class);
        given(connection.getMetaData()).willReturn(metaData);

        assertEquals(serviceResolver.resolve(connection), service);

        verify(serviceResolver).resolve(eq(metaData));
        verify(serviceResolver).resolve(any(DatabaseInfo.class));
        verify(serviceResolver).resolveService(any(DatabaseInfo.class));
        verify(serviceResolver, never()).resolveServiceClass(any(DatabaseInfo.class));
    }
}
