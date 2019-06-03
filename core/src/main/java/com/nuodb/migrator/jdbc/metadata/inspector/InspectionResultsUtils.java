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
package com.nuodb.migrator.jdbc.metadata.inspector;

import com.nuodb.migrator.jdbc.metadata.Catalog;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Identifier;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Table;

import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;
import static com.nuodb.migrator.jdbc.metadata.MetaDataType.DATABASE;

/**
 * @author Sergey Bushik
 */
public class InspectionResultsUtils {

    public static Database addDatabase(InspectionResults results) {
        Database database = results.getObject(DATABASE);
        if (database == null) {
            results.addObject(database = new Database());
        }
        return database;
    }

    public static Catalog addCatalog(InspectionResults results, String catalogName) {
        return addCatalog(results, catalogName, true);
    }

    public static Catalog addCatalog(InspectionResults results, String catalogName, boolean addObject) {
        Database database = addDatabase(results);
        Catalog catalog;
        Identifier catalogId = valueOf(catalogName);
        if (database.hasCatalog(catalogId)) {
            catalog = database.getCatalog(catalogId);
        } else {
            catalog = database.addCatalog(catalogId);
            if (addObject) {
                results.addObject(catalog);
            }
        }
        return catalog;
    }

    public static Schema addSchema(InspectionResults results, String catalogName, String schemaName) {
        return addSchema(results, catalogName, schemaName, true);
    }

    public static Schema addSchema(InspectionResults results, String catalogName, String schemaName,
            boolean addObject) {
        Catalog catalog = addCatalog(results, catalogName, addObject);
        Schema schema;
        Identifier schemaId = valueOf(schemaName);
        if (catalog.hasSchema(schemaId)) {
            schema = catalog.getSchema(schemaId);
        } else {
            schema = catalog.addSchema(schemaId);
            if (addObject) {
                results.addObject(schema);
            }
        }
        return schema;
    }

    public static Table addTable(InspectionResults results, String catalogName, String schemaName, String tableName) {
        return addTable(results, catalogName, schemaName, tableName, true);
    }

    public static Table addTable(InspectionResults results, String catalogName, String schemaName, String tableName,
            boolean addObject) {
        Schema schema = addSchema(results, catalogName, schemaName, addObject);
        Table table;
        Identifier tableId = valueOf(tableName);
        if (schema.hasTable(tableId)) {
            table = schema.getTable(tableId);
        } else {
            table = schema.addTable(tableId);
            if (addObject) {
                results.addObject(table);
            }
        }
        return table;
    }
}
