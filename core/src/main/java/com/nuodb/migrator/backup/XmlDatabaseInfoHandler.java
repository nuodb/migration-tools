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

import com.nuodb.migrator.jdbc.metadata.DatabaseInfo;
import com.nuodb.migrator.jdbc.metadata.NuoDBDatabaseInfo;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlReadTargetAwareContext;
import com.nuodb.migrator.utils.xml.XmlReadWriteHandlerBase;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("all")
public class XmlDatabaseInfoHandler extends XmlReadWriteHandlerBase<DatabaseInfo> {

    private static final String PRODUCT_NAME_ELEMENT = "product-name";
    private static final String PRODUCT_VERSION_ELEMENT = "product-version";
    private static final String MAJOR_VERSION_ELEMENT = "major-version";
    private static final String MINOR_VERSION_ELEMENT = "minor-version";
    private static final String PLATFORM_VERSION_ELEMENT = "platform-version";

    public XmlDatabaseInfoHandler() {
        super(DatabaseInfo.class);
    }

    @Override
    protected void readElement(InputNode input, DatabaseInfo databaseInfo, XmlReadContext context) throws Exception {
        final String element = input.getName();
        if (PRODUCT_NAME_ELEMENT.equals(element)) {
            databaseInfo.setProductName(context.read(input, String.class));
        } else if (PRODUCT_VERSION_ELEMENT.equals(element)) {
            databaseInfo.setProductVersion(context.read(input, String.class));
        } else if (MAJOR_VERSION_ELEMENT.equals(element)) {
            databaseInfo.setMajorVersion(context.read(input, int.class));
        } else if (MINOR_VERSION_ELEMENT.equals(element)) {
            databaseInfo.setMinorVersion(context.read(input, int.class));
        } else if (PLATFORM_VERSION_ELEMENT.equals(element)) {
            ((XmlReadTargetAwareContext) context)
                    .setTarget(databaseInfo = new NuoDBDatabaseInfo(databaseInfo, context.read(input, int.class)));
        }
    }

    @Override
    protected void writeElements(DatabaseInfo databaseInfo, OutputNode output, XmlWriteContext context)
            throws Exception {
        context.writeElement(output, PRODUCT_NAME_ELEMENT, databaseInfo.getProductName());
        context.writeElement(output, PRODUCT_VERSION_ELEMENT, databaseInfo.getProductVersion());
        context.writeElement(output, MAJOR_VERSION_ELEMENT, databaseInfo.getMajorVersion());
        context.writeElement(output, MINOR_VERSION_ELEMENT, databaseInfo.getMinorVersion());
        if (databaseInfo instanceof NuoDBDatabaseInfo) {
            Integer platformVersion = ((NuoDBDatabaseInfo) databaseInfo).getPlatformVersion();
            context.writeElement(output, PLATFORM_VERSION_ELEMENT, platformVersion);
        }
    }

    @Override
    public boolean canWrite(Object source, Class type, OutputNode output, XmlWriteContext context) {
        return type != null && getType().isAssignableFrom(type);
    }
}
