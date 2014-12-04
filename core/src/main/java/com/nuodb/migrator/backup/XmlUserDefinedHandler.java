/**
 * Copyright (c) 2014, NuoDB, Inc.
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


import com.nuodb.migrator.jdbc.metadata.UserDefined;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlWriteContext;

import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

/**
 * @author Mukund
 */
public class XmlUserDefinedHandler extends XmlIdentifiableHandlerBase<UserDefined> {

    private static final String TYPE_NAME = "object_name";
    private static final String TYPE_CODE = "typecode";

    public XmlUserDefinedHandler() {
        super(UserDefined.class);
    }

    @Override
    protected void readAttributes(InputNode input, UserDefined userDefined, XmlReadContext context) throws Exception {
        userDefined.setUserDefinedName(context.readAttribute(input, TYPE_NAME, String.class));
        userDefined.setTypeCode(context.readAttribute(input, TYPE_CODE, String.class));
    }

    @Override
    protected void writeAttributes(UserDefined userDefined, OutputNode output, XmlWriteContext context) throws Exception {
        if (!userDefined.getUserDefinedName().equalsIgnoreCase(null) && !userDefined.getTypeCode().equalsIgnoreCase(null)) {
            context.writeAttribute(output, TYPE_NAME, userDefined.getUserDefinedName());
            context.writeAttribute(output, TYPE_CODE, userDefined.getTypeCode());
        }
    }
}
