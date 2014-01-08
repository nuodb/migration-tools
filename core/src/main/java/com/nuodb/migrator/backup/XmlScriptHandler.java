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

import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlReadWriteHandlerBase;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

/**
 * @author Sergey Bushik
 */
public class XmlScriptHandler extends XmlReadWriteHandlerBase<Script> implements XmlConstants {

    private static final String CATALOG_NAME = "catalog-name";
    private static final String SCHEMA_NAME = "schema-name";
    private static final String NAME_ATTRIBUTE = "name";
    private static final String SCRIPT_COUNT_ATTRIBUTE = "script-count";

    public XmlScriptHandler() {
        super(Script.class);
    }

    @Override
    protected void readAttributes(InputNode input, Script script, XmlReadContext context) throws Exception {
        script.setCatalogName(context.readAttribute(input, CATALOG_NAME, String.class));
        script.setSchemaName(context.readAttribute(input, SCHEMA_NAME, String.class));
        script.setName(context.readAttribute(input, NAME_ATTRIBUTE, String.class));
        Long scriptCount = context.readAttribute(input, SCRIPT_COUNT_ATTRIBUTE, Long.class);
        script.setScriptCount(scriptCount != null ? scriptCount : 0);
    }

    @Override
    protected void writeAttributes(Script script, OutputNode output, XmlWriteContext context) throws Exception {
        if (script.getCatalogName() != null) {
            context.writeAttribute(output, CATALOG_NAME, script.getCatalogName());
        }
        if (script.getSchemaName() != null) {
            context.writeAttribute(output, SCHEMA_NAME, script.getSchemaName());
        }
        context.writeAttribute(output, NAME_ATTRIBUTE, script.getName());
        context.writeAttribute(output, SCRIPT_COUNT_ATTRIBUTE, script.getScriptCount());
    }
}
