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

import com.nuodb.migrator.spec.ConnectionSpec;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlReadWriteHandlerBase;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import static com.nuodb.migrator.spec.ConnectionSpecBase.DEFAULT_AUTO_COMMIT;
import static com.nuodb.migrator.utils.xml.XmlAliasTypeMapper.TYPE_ATTRIBUTE;

/**
 * @author Sergey Bushik
 */
public abstract class XmlConnectionSpecHandler<T extends ConnectionSpec> extends XmlReadWriteHandlerBase<T>
        implements XmlConstants {

    private static final String CATALOG_ATTRIBUTE = "catalog";
    private static final String SCHEMA_ATTRIBUTE = "schema";
    private static final String AUTO_COMMIT_ATTRIBUTE = "auto-commit";
    private static final String TRANSACTION_ISOLATION_ATTRIBUTE = "transaction-isolation";

    private final String typeAttribute;

    protected XmlConnectionSpecHandler(Class<? extends T> type, String typeAttribute) {
        super(type);
        this.typeAttribute = typeAttribute;
    }

    @Override
    protected void readAttributes(InputNode input, T target, XmlReadContext context) throws Exception {
        target.setType(context.readAttribute(input, TYPE_ATTRIBUTE, String.class));
        target.setCatalog(context.readAttribute(input, CATALOG_ATTRIBUTE, String.class));
        target.setSchema(context.readAttribute(input, SCHEMA_ATTRIBUTE, String.class));
        target.setAutoCommit(context.readAttribute(input, AUTO_COMMIT_ATTRIBUTE, Boolean.class, DEFAULT_AUTO_COMMIT));
        target.setTransactionIsolation(context.readAttribute(input, TRANSACTION_ISOLATION_ATTRIBUTE, Integer.class));
    }

    @Override
    protected void writeAttributes(T target, OutputNode output, XmlWriteContext context) throws Exception {
        context.writeAttribute(output, TYPE_ATTRIBUTE, getTypeAttribute());
        if (target.getCatalog() != null) {
            context.writeAttribute(output, CATALOG_ATTRIBUTE, target.getCatalog());
        }
        if (target.getSchema() != null) {
            context.writeAttribute(output, SCHEMA_ATTRIBUTE, target.getSchema());
        }
        Boolean autoCommit = target.getAutoCommit();
        if (autoCommit != null && autoCommit) {
            context.writeAttribute(output, AUTO_COMMIT_ATTRIBUTE, autoCommit);
        }
        if (target.getTransactionIsolation() != null) {
            context.writeAttribute(output, TRANSACTION_ISOLATION_ATTRIBUTE, target.getTransactionIsolation());
        }
    }

    protected String getTypeAttribute() {
        return typeAttribute;
    }
}
