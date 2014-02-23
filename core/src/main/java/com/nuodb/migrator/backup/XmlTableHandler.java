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
package com.nuodb.migrator.backup;

import com.nuodb.migrator.jdbc.metadata.Check;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.ForeignKey;
import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.PrimaryKey;
import com.nuodb.migrator.jdbc.metadata.Schema;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.spec.MetaDataSpec;
import com.nuodb.migrator.spec.TableSpec;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlReadTargetAwareContext;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import java.util.Collection;

import static com.nuodb.migrator.spec.MetaDataSpec.TABLE_TYPES;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static org.apache.commons.lang3.ArrayUtils.indexOf;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("ConstantConditions")
public class XmlTableHandler extends XmlIdentifiableHandlerBase<Table> {

    private static final String TYPE_ATTRIBUTE = "type";
    private static final String COMMENT_ELEMENT = "comment";
    private static final String COLUMN_ELEMENT = "column";
    private static final String PRIMARY_KEY_ELEMENT = "primary-key";
    private static final String INDEX_ELEMENT = "index";
    private static final String FOREIGN_KEY = "foreign-key";
    private static final String CHECK_ELEMENT = "check";

    public XmlTableHandler() {
        super(Table.class);
    }

    @Override
    protected Table createTarget(InputNode input, Class<? extends Table> type) {
        return null;
    }

    @Override
    protected void readAttributes(InputNode input, XmlReadTargetAwareContext<Table> context) throws Exception {
        Schema schema = getParent(context);
        String name = context.readAttribute(input, NAME_ATTRIBUTE, String.class);
        Table table = schema.hasTable(name) ? schema.getTable(name) : schema.addTable(name);
        table.setType(context.readAttribute(input, TYPE_ATTRIBUTE, String.class));
        context.setTarget(table);
    }

    @Override
    protected void readElement(InputNode input, Table table, XmlReadContext context) throws Exception {
        String element = input.getName();
        if (COLUMN_ELEMENT.equals(element)) {
            table.addColumn(context.read(input, Column.class));
        } else if (PRIMARY_KEY_ELEMENT.equals(element)) {
            table.setPrimaryKey(context.read(input, PrimaryKey.class));
        } else if (INDEX_ELEMENT.equals(element)) {
            table.addIndex(context.read(input, Index.class));
        } else if (FOREIGN_KEY.equals(element)) {
            table.addForeignKey(context.read(input, ForeignKey.class));
        } else if (COMMENT_ELEMENT.equals(element)) {
            table.setComment(context.read(input, String.class));
        } else if (CHECK_ELEMENT.equals(element)) {
            table.addCheck(context.read(input, Check.class));
        }
    }

    @Override
    protected boolean skip(Table table, MetaDataSpec metaDataSpec) {
        boolean skip = super.skip(table, metaDataSpec);
        if (!skip) {
            String[] tableTypes = metaDataSpec != null ? metaDataSpec.getTableTypes() : TABLE_TYPES;
            skip = indexOf(tableTypes, table.getType()) == -1;
        }
        if (!skip) {
            Collection<TableSpec> tableSpecs = metaDataSpec != null ? metaDataSpec.getTableSpecs() : null;
            if (!isEmpty(tableSpecs)) {
                skip = true;
                Database database = table.getDatabase();
                for (TableSpec tableSpec : tableSpecs) {
                    if (database.findTable(tableSpec.getTable()).equals(table)) {
                        skip = false;
                        break;
                    }
                }
            }
        }
        return skip;
    }

    @Override
    protected void writeAttributes(Table table, OutputNode output, XmlWriteContext context) throws Exception {
        context.writeAttribute(output, NAME_ATTRIBUTE, table.getName());
        context.writeAttribute(output, TYPE_ATTRIBUTE, table.getType());
    }

    @Override
    protected void writeElements(Table table, OutputNode output, XmlWriteContext context) throws Exception {
        if (table.getComment() != null) {
            context.writeElement(output, COMMENT_ELEMENT, table.getComment());
        }
        for (Column column : table.getColumns()) {
            context.writeElement(output, COLUMN_ELEMENT, column);
        }
        PrimaryKey primaryKey = table.getPrimaryKey();
        if (primaryKey != null) {
            context.writeElement(output, PRIMARY_KEY_ELEMENT, primaryKey);
        }
        for (Index index : table.getIndexes()) {
            if (!index.isPrimary()) {
                context.writeElement(output, INDEX_ELEMENT, index);
            }
        }
        for (ForeignKey foreignKey : table.getForeignKeys()) {
            context.writeElement(output, FOREIGN_KEY, foreignKey);
        }
        for (Check check : table.getChecks()) {
            context.writeElement(output, CHECK_ELEMENT, check);
        }
    }
}
