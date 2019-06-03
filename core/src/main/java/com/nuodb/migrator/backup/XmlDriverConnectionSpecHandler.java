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

import com.nuodb.migrator.spec.DriverConnectionSpec;
import com.nuodb.migrator.utils.Collections;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class XmlDriverConnectionSpecHandler extends XmlConnectionSpecHandler<DriverConnectionSpec> {

    private static final String DRIVER_ELEMENT = "driver";
    private static final String URL_ELEMENT = "url";
    private static final String USERNAME_ELEMENT = "username";
    private static final String PROPERTY_ELEMENT = "property";
    private static final String KEY_ATTRIBUTE = "key";
    private static final String VALUE_ATTRIBUTE = "value";

    public XmlDriverConnectionSpecHandler() {
        super(DriverConnectionSpec.class, DRIVER_TYPE);
    }

    @Override
    protected void readElement(InputNode input, DriverConnectionSpec connectionSpec, XmlReadContext context)
            throws Exception {
        String element = input.getName();
        if (DRIVER_ELEMENT.equals(element)) {
            connectionSpec.setDriver(context.read(input, String.class));
        } else if (URL_ELEMENT.equals(element)) {
            connectionSpec.setUrl(context.read(input, String.class));
        } else if (USERNAME_ELEMENT.equals(element)) {
            connectionSpec.setUsername(context.read(input, String.class));
        } else if (PROPERTY_ELEMENT.equals(element)) {
            connectionSpec.addProperty(context.readAttribute(input, KEY_ATTRIBUTE, String.class),
                    context.readAttribute(input, VALUE_ATTRIBUTE, String.class));
        }
    }

    @Override
    protected void writeElements(DriverConnectionSpec connectionSpec, OutputNode output, XmlWriteContext context)
            throws Exception {
        super.writeElements(connectionSpec, output, context);
        context.writeElement(output, DRIVER_ELEMENT, connectionSpec.getDriver());
        context.writeElement(output, URL_ELEMENT, connectionSpec.getUrl());
        context.writeElement(output, USERNAME_ELEMENT, connectionSpec.getUsername());
        Map<String, Object> properties = connectionSpec.getProperties();
        if (!Collections.isEmpty(properties)) {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                OutputNode property = output.getChild(PROPERTY_ELEMENT);
                context.writeAttribute(property, KEY_ATTRIBUTE, entry.getKey());
                context.writeAttribute(property, VALUE_ATTRIBUTE, entry.getValue());
            }
        }
    }
}
