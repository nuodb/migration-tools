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
package com.nuodb.migrator.backup.catalog;

import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

/**
 * @author Sergey Bushik
 */
public class XmlTableRowSetHandler extends XmlRowSetHandler<TableRowSet> {

    private static final String CATALOG_NAME = "catalog-name";
    private static final String SCHEMA_NAME = "schema-name";
    private static final String TABLE_NAME = "table-name";

    public XmlTableRowSetHandler() {
        super(TableRowSet.class, TABLE_TYPE);
    }

    @Override
    protected void readAttributes(InputNode input, TableRowSet rowSet, XmlReadContext context) throws Exception {
        super.readAttributes(input, rowSet, context);
        rowSet.setCatalogName(context.readAttribute(input, CATALOG_NAME, String.class));
        rowSet.setSchemaName(context.readAttribute(input, SCHEMA_NAME, String.class));
        rowSet.setTableName(context.readAttribute(input, TABLE_NAME, String.class));
    }

    @Override
    protected void writeAttributes(TableRowSet rowSet, OutputNode output, XmlWriteContext context) throws Exception {
        super.writeAttributes(rowSet, output, context);
        if (rowSet.getCatalogName() != null) {
            context.writeAttribute(output, CATALOG_NAME, rowSet.getCatalogName());
        }
        if (rowSet.getSchemaName() != null) {
            context.writeAttribute(output, SCHEMA_NAME, rowSet.getSchemaName());
        }
        context.writeAttribute(output, TABLE_NAME, rowSet.getTableName());
    }
}
