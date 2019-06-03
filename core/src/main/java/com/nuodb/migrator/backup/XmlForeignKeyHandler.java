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
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.Deferrability;
import com.nuodb.migrator.jdbc.metadata.ForeignKey;
import com.nuodb.migrator.jdbc.metadata.ForeignKeyReference;
import com.nuodb.migrator.jdbc.metadata.ReferenceAction;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import static com.nuodb.migrator.backup.XmlTableHandler.getTableBinding;
import static com.nuodb.migrator.utils.StringUtils.lowerCase;
import static com.nuodb.migrator.utils.StringUtils.upperCase;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("all")
public class XmlForeignKeyHandler extends XmlIdentifiableHandlerBase<ForeignKey> {

    private static String REFERENCE_ELEMENT = "reference";
    private static String UPDATE_ACTION_ATTRIBUTE = "update-action";
    private static String DELETE_ACTION_ATTRIBUTE = "delete-action";
    private static String DEFERRABILITY_ATTRIBUTE = "deferrability";
    private static String PRIMARY_CATALOG_ATTRIBUTE = "primary-catalog";
    private static String FOREIGN_CATALOG_ATTRIBUTE = "foreign-catalog";
    private static String PRIMARY_SCHEMA_ATTRIBUTE = "primary-schema";
    private static String FOREIGN_SCHEMA_ATTRIBUTE = "foreign-schema";
    private static String PRIMARY_TABLE_ATTRIBUTE = "primary-table";
    private static String FOREIGN_TABLE_ATTRIBUTE = "foreign-table";
    private static String PRIMARY_COLUMN_ATTRIBUTE = "primary-column";
    private static String FOREIGN_COLUMN_ATTRIBUTE = "foreign-column";

    public XmlForeignKeyHandler() {
        super(ForeignKey.class);
    }

    @Override
    protected void readAttributes(InputNode input, ForeignKey foreignKey, XmlReadContext context) throws Exception {
        super.readAttributes(input, foreignKey, context);
        // 4 levels up to reach out to database from foreign key -> table ->
        // schema -> catalog -> database
        Database database = getParent(context, 4);

        Catalog primaryCatalog = database
                .addCatalog(context.readAttribute(input, PRIMARY_CATALOG_ATTRIBUTE, String.class));
        Schema primarySchema = primaryCatalog
                .addSchema(context.readAttribute(input, PRIMARY_SCHEMA_ATTRIBUTE, String.class));
        Table primaryTable = primarySchema
                .addTable(context.readAttribute(input, PRIMARY_TABLE_ATTRIBUTE, String.class));
        foreignKey.setPrimaryTable(primaryTable);

        // adds primary tables created by foreign key declaration
        getTableBinding(primaryTable, context).addReference(foreignKey);

        Catalog foreignCatalog = database
                .addCatalog(context.readAttribute(input, FOREIGN_CATALOG_ATTRIBUTE, String.class));
        Schema foreignSchema = foreignCatalog
                .addSchema(context.readAttribute(input, FOREIGN_SCHEMA_ATTRIBUTE, String.class));
        Table foreignTable = foreignSchema
                .addTable(context.readAttribute(input, FOREIGN_TABLE_ATTRIBUTE, String.class));
        foreignKey.setForeignTable(foreignTable);

        String updateAction = context.readAttribute(input, UPDATE_ACTION_ATTRIBUTE, String.class);
        if (updateAction != null) {
            foreignKey.setUpdateAction(ReferenceAction.valueOf(upperCase(updateAction)));
        }
        String deleteAction = context.readAttribute(input, DELETE_ACTION_ATTRIBUTE, String.class);
        if (deleteAction != null) {
            foreignKey.setDeleteAction(ReferenceAction.valueOf(upperCase(deleteAction)));
        }
        String deferrability = context.readAttribute(input, DEFERRABILITY_ATTRIBUTE, String.class);
        if (deferrability != null) {
            foreignKey.setDeferrability(Deferrability.valueOf(upperCase(deferrability)));
        }
    }

    @Override
    protected void readElement(InputNode input, ForeignKey foreignKey, XmlReadContext context) throws Exception {
        String element = input.getName();
        if (REFERENCE_ELEMENT.equals(element)) {
            readReference(input, foreignKey, context);
        }
    }

    protected void readReference(InputNode input, ForeignKey foreignKey, XmlReadContext context) {
        Column primaryColumn = foreignKey.getPrimaryTable()
                .addColumn(context.readAttribute(input, PRIMARY_COLUMN_ATTRIBUTE, String.class));
        Column foreignColumn = foreignKey.getForeignTable()
                .addColumn(context.readAttribute(input, FOREIGN_COLUMN_ATTRIBUTE, String.class));
        foreignKey.addReference(primaryColumn, foreignColumn);
    }

    @Override
    protected void writeAttributes(ForeignKey foreignKey, OutputNode output, XmlWriteContext context) throws Exception {
        super.writeAttributes(foreignKey, output, context);
        Table primaryTable = foreignKey.getPrimaryTable();
        Catalog primaryCatalog = primaryTable.getCatalog();
        if (primaryCatalog != null && primaryCatalog.getName() != null) {
            context.writeAttribute(output, PRIMARY_CATALOG_ATTRIBUTE, primaryCatalog.getName());
        }
        Schema primarySchema = primaryTable.getSchema();
        if (primarySchema != null && primarySchema.getName() != null) {
            context.writeAttribute(output, PRIMARY_SCHEMA_ATTRIBUTE, primarySchema.getName());
        }
        context.writeAttribute(output, PRIMARY_TABLE_ATTRIBUTE, primaryTable.getName());
        Table foreignTable = foreignKey.getForeignTable();
        Catalog foreignCatalog = foreignTable.getCatalog();
        if (foreignCatalog != null && foreignCatalog.getName() != null) {
            context.writeAttribute(output, FOREIGN_CATALOG_ATTRIBUTE, foreignCatalog.getName());
        }
        Schema foreignSchema = foreignTable.getSchema();
        if (foreignSchema != null && foreignSchema.getName() != null) {
            context.writeAttribute(output, FOREIGN_SCHEMA_ATTRIBUTE, foreignSchema.getName());
        }
        context.writeAttribute(output, FOREIGN_TABLE_ATTRIBUTE, foreignTable.getName());
        ReferenceAction updateAction = foreignKey.getUpdateAction();
        if (updateAction != null) {
            context.writeAttribute(output, UPDATE_ACTION_ATTRIBUTE, lowerCase(updateAction.toString()));
        }
        ReferenceAction deleteAction = foreignKey.getDeleteAction();
        if (deleteAction != null) {
            context.writeAttribute(output, DELETE_ACTION_ATTRIBUTE, lowerCase(deleteAction.toString()));
        }
        Deferrability deferrability = foreignKey.getDeferrability();
        if (deferrability != null) {
            context.writeAttribute(output, DEFERRABILITY_ATTRIBUTE, lowerCase(deferrability.toString()));
        }
    }

    @Override
    protected void writeElements(ForeignKey foreignKey, OutputNode output, XmlWriteContext context) throws Exception {
        for (ForeignKeyReference reference : foreignKey.getReferences()) {
            writeReference(reference, output.getChild(REFERENCE_ELEMENT), context);
        }
    }

    protected void writeReference(ForeignKeyReference reference, OutputNode output, XmlWriteContext context)
            throws Exception {
        context.writeAttribute(output, PRIMARY_COLUMN_ATTRIBUTE, reference.getPrimaryColumn().getName());
        context.writeAttribute(output, FOREIGN_COLUMN_ATTRIBUTE, reference.getForeignColumn().getName());
    }
}
