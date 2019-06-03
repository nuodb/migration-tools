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

import com.google.common.base.Function;
import com.nuodb.migrator.jdbc.metadata.Constraint;
import com.nuodb.migrator.jdbc.metadata.Database;
import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlReadWriteHandlerBase;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import java.util.Collection;

import static com.google.common.collect.Iterables.transform;
import static com.nuodb.migrator.backup.XmlTableHandler.getTableBindings;
import static com.nuodb.migrator.utils.Collections.isEmpty;
import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class XmlBackupHandler extends XmlReadWriteHandlerBase<Backup> implements XmlConstants {

    private static final String VERSION_ATTRIBUTE = "version";
    private static final String FORMAT_ATTRIBUTE = "format";
    private static final String DATABASE_ELEMENT = "database";
    private static final String DATABASE_INFO_ELEMENT = "database-info";

    public XmlBackupHandler() {
        super(Backup.class);
    }

    @Override
    public Backup read(InputNode input, Class<? extends Backup> type, XmlReadContext context) {
        Backup backup = super.read(input, type, context);
        processTableBindings(getTableBindings(context));
        return backup;
    }

    protected void processTableBindings(TableBindings tableBindings) {
        for (TableBinding tableBinding : tableBindings) {
            Table table = tableBinding.getTable();
            Collection<Constraint> references = tableBinding.getReferences();
            if (!tableBinding.isDeclared() && !isEmpty(references)) {
                Iterable<String> referenced = transform(references, new Function<Constraint, String>() {
                    @Override
                    public String apply(Constraint constraint) {
                        return constraint.getQualifiedName();
                    }
                });
                if (logger.isWarnEnabled()) {
                    logger.warn(format("Ignoring table %s, which is referenced by %s constraints, "
                            + "but is not declared explicitly", table.getQualifiedName(), referenced));
                }
                table.getSchema().removeTable(table);
            }
        }
    }

    @Override
    protected void readAttributes(InputNode input, Backup backup, XmlReadContext context) throws Exception {
        backup.setVersion(context.readAttribute(input, VERSION_ATTRIBUTE, String.class));
        backup.setFormat(context.readAttribute(input, FORMAT_ATTRIBUTE, String.class));
    }

    @Override
    protected void readElement(InputNode input, Backup backup, XmlReadContext context) throws Exception {
        String element = input.getName();
        if (DATABASE_ELEMENT.equals(element)) {
            backup.setDatabase(context.read(input, Database.class));
        } else if (ROW_SET.equals(element)) {
            backup.addRowSet(context.read(input, RowSet.class));
        } else if (DATABASE_INFO_ELEMENT.equals(element)) {
            // old format support
            Database database = new Database();
            database.setDatabaseInfo(context.read(input, DatabaseInfo.class));
            backup.setDatabase(database);
        }
    }

    @Override
    protected void writeAttributes(Backup backup, OutputNode output, XmlWriteContext context) throws Exception {
        context.writeAttribute(output, VERSION_ATTRIBUTE, backup.getVersion());
        context.writeAttribute(output, FORMAT_ATTRIBUTE, backup.getFormat());
    }

    @Override
    protected void writeElements(Backup backup, OutputNode output, XmlWriteContext context) throws Exception {
        if (backup.getDatabase() != null) {
            context.writeElement(output, DATABASE_ELEMENT, backup.getDatabase());
        }
        Collection<RowSet> rowSets = backup.getRowSets();
        if (!isEmpty(rowSets)) {
            for (RowSet rowSet : rowSets) {
                context.writeElement(output, ROW_SET, rowSet);
            }
        }
    }
}
