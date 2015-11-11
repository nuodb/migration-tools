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

import com.nuodb.migrator.jdbc.dialect.DialectResolver;
import com.nuodb.migrator.jdbc.metadata.Catalog;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.DriverInfo;
import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import static com.nuodb.migrator.context.ContextUtils.createService;

/**
 * @author Sergey Bushik
 */
public class XmlDatabaseHandler extends XmlIdentifiableHandlerBase<Database> implements XmlConstants {

    private static final String DRIVER_INFO_ELEMENT = "driver-info";
    private static final String DATABASE_INFO_ELEMENT = "database-info";
    private static final String CATALOG_ELEMENT = "catalog";
    private static final String CONNECTION_SPEC_ELEMENT = "connection-spec";

    public XmlDatabaseHandler() {
        super(Database.class);
    }

    @Override
    protected void readElement(InputNode input, Database database, XmlReadContext context) throws Exception {
        final String element = input.getName();
        if (DRIVER_INFO_ELEMENT.equals(element)) {
            database.setDriverInfo(context.read(input, DriverInfo.class));
        } else if (DATABASE_INFO_ELEMENT.equals(element)) {
            DatabaseInfo databaseInfo = context.read(input, DatabaseInfo.class);
            database.setDatabaseInfo(databaseInfo);
            database.setDialect(createDialectResolver().resolve(databaseInfo));
        } else if (CONNECTION_SPEC_ELEMENT.equals(element)) {
            database.setConnectionSpec(context.read(input, ConnectionSpec.class));
        } else if (CATALOG_ELEMENT.equals(element)) {
            database.addCatalog(context.read(input, Catalog.class));
        }
    }

    @Override
    protected void writeElements(Database database, OutputNode output, XmlWriteContext context) throws Exception {
        DriverInfo driverInfo = database.getDriverInfo();
        if (driverInfo != null) {
            context.writeElement(output, DRIVER_INFO_ELEMENT, driverInfo);
        }
        DatabaseInfo databaseInfo = database.getDatabaseInfo();
        if (databaseInfo != null) {
            context.writeElement(output, DATABASE_INFO_ELEMENT, databaseInfo);
        }
        ConnectionSpec connectionSpec = database.getConnectionSpec();
        if (connectionSpec != null) {
            context.writeElement(output, CONNECTION_SPEC_ELEMENT, connectionSpec);
        }
        for (Catalog catalog : database.getCatalogs()) {
            context.writeElement(output, CATALOG_ELEMENT, catalog);
        }
    }

    protected DialectResolver createDialectResolver() {
        return createService(DialectResolver.class);
    }
}
