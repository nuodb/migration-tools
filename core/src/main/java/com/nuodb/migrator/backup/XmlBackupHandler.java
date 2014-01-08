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

import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlReadWriteHandlerBase;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

/**
 * @author Sergey Bushik
 */
public class XmlBackupHandler extends XmlReadWriteHandlerBase<Backup> implements XmlConstants {

    private static final String VERSION_ATTRIBUTE = "version";
    private static final String FORMAT_ATTRIBUTE = "format";
    private static final String DATABASE_INFO_ELEMENT = "database-info";
    private static final String SCRIPT_ELEMENT = "script";

    public XmlBackupHandler() {
        super(Backup.class);
    }

    @Override
    protected void readAttributes(InputNode input, Backup backup, XmlReadContext context) throws Exception {
        backup.setVersion(context.readAttribute(input, VERSION_ATTRIBUTE, String.class));
        backup.setFormat(context.readAttribute(input, FORMAT_ATTRIBUTE, String.class));
    }

    @Override
    protected void readElement(InputNode input, Backup backup, XmlReadContext context) throws Exception {
        if (DATABASE_INFO_ELEMENT.equals(input.getName())) {
            backup.setDatabaseInfo(context.read(input, DatabaseInfo.class));
        } else if (SCRIPT_ELEMENT.equals(input.getName())) {
            backup.addScript(context.read(input, Script.class));
        } else if (ROW_SET.equals(input.getName())) {
            backup.addRowSet(context.read(input, RowSet.class));
        }
    }

    @Override
    protected void writeAttributes(Backup backup, OutputNode output, XmlWriteContext context) throws Exception {
        context.writeAttribute(output, VERSION_ATTRIBUTE, backup.getVersion());
        context.writeAttribute(output, FORMAT_ATTRIBUTE, backup.getFormat());
    }

    @Override
    protected void writeElements(Backup backup, OutputNode output, XmlWriteContext context) throws Exception {
        if (backup.getDatabaseInfo() != null) {
            context.writeElement(output, DATABASE_INFO_ELEMENT, backup.getDatabaseInfo());
        }
        if (backup.getScripts() != null) {
            for (Script script : backup.getScripts()) {
                context.writeElement(output, SCRIPT_ELEMENT, script);
            }
        }
        for (RowSet rowSet : backup.getRowSets()) {
            context.writeElement(output, ROW_SET, rowSet);
        }
    }
}

