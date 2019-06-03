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

import com.nuodb.migrator.jdbc.metadata.DriverInfo;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlReadWriteHandlerBase;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

/**
 * @author Sergey Bushik
 */
public class XmlDriverInfoHandler extends XmlReadWriteHandlerBase<DriverInfo> {

    private static final String NAME_ELEMENT = "name";
    private static final String VERSION_ELEMENT = "version";
    private static final String MAJOR_VERSION_ELEMENT = "major-version";
    private static final String MINOR_VERSION_ELEMENT = "minor-version";

    public XmlDriverInfoHandler() {
        super(DriverInfo.class);
    }

    @Override
    protected void readElement(InputNode input, DriverInfo driverInfo, XmlReadContext context) throws Exception {
        final String element = input.getName();
        if (NAME_ELEMENT.equals(element)) {
            driverInfo.setName(context.read(input, String.class));
        } else if (VERSION_ELEMENT.equals(element)) {
            driverInfo.setVersion(context.read(input, String.class));
        } else if (MAJOR_VERSION_ELEMENT.equals(element)) {
            driverInfo.setMajorVersion(context.read(input, int.class));
        } else if (MINOR_VERSION_ELEMENT.equals(element)) {
            driverInfo.setMinorVersion(context.read(input, int.class));
        }
    }

    @Override
    protected void writeElements(DriverInfo driverInfo, OutputNode output, XmlWriteContext context) throws Exception {
        context.writeElement(output, NAME_ELEMENT, driverInfo.getName());
        context.writeElement(output, VERSION_ELEMENT, driverInfo.getVersion());
        context.writeElement(output, MAJOR_VERSION_ELEMENT, driverInfo.getMajorVersion());
        context.writeElement(output, MINOR_VERSION_ELEMENT, driverInfo.getMinorVersion());
    }
}