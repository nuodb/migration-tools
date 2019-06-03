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

import com.nuodb.migrator.jdbc.metadata.Catalog;
import com.nuodb.migrator.jdbc.metadata.Identifier;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Sequence;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.jdbc.metadata.UserDefinedType;
import com.nuodb.migrator.jdbc.metadata.inspector.SchemaInspectionScope;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import static com.nuodb.migrator.jdbc.metadata.Identifier.valueOf;

/**
 * @author Sergey Bushik
 */
public class XmlSchemaHandler extends XmlIdentifiableHandlerBase<Schema> implements XmlConstants {

    private static final String TABLE_ELEMENT = "table";
    private static final String SEQUENCE_ELEMENT = "sequence";
    private static final String USER_DEFINED_TYPE_ELEMENT = "user-defined-type";

    public XmlSchemaHandler() {
        super(Schema.class);
    }

    @Override
    protected Schema createTarget(InputNode input, Class<? extends Schema> type, XmlReadContext context) {
        Catalog catalog = getParent(context, 0);
        String name = context.readAttribute(input, NAME_ATTRIBUTE, String.class);
        return catalog != null ? (catalog.hasSchema(name) ? catalog.getSchema(name) : catalog.addSchema(name))
                : new Schema(name);
    }

    @Override
    protected void readElement(InputNode input, Schema schema, XmlReadContext context) throws Exception {
        String element = input.getName();
        if (TABLE_ELEMENT.equals(element)) {
            schema.addTable(context.read(input, Table.class));
        } else if (SEQUENCE_ELEMENT.equals(element)) {
            schema.addSequence(context.read(input, Sequence.class));
        } else if (USER_DEFINED_TYPE_ELEMENT.equals(element)) {
            schema.addUserDefinedType(context.read(input, UserDefinedType.class));
        }
    }

    @Override
    protected void writeElements(Schema schema, OutputNode output, final XmlWriteContext context) throws Exception {
        for (Sequence sequence : schema.getSequences()) {
            context.writeElement(output, SEQUENCE_ELEMENT, sequence);
        }
        for (UserDefinedType userDefinedType : schema.getUserDefinedTypes()) {
            context.writeElement(output, USER_DEFINED_TYPE_ELEMENT, userDefinedType);
        }
        for (Table table : schema.getTables()) {
            context.writeElement(output, TABLE_ELEMENT, table);
        }
    }

    @Override
    protected boolean skip(Schema schema, XmlWriteContext context) {
        SchemaInspectionScope schemaInspectionScope = getInspectionScope(context);
        Identifier catalogId = valueOf(schemaInspectionScope != null ? schemaInspectionScope.getCatalog() : null);
        Identifier schemaId = valueOf(schemaInspectionScope != null ? schemaInspectionScope.getSchema() : null);
        return super.skip(schema, context)
                || (catalogId != null && !schema.getCatalog().getIdentifier().equals(catalogId))
                || (schemaId != null && !schema.getIdentifier().equals(schemaId));
    }
}
