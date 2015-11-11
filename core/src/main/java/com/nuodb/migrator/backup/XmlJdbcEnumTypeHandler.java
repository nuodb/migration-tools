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

import com.nuodb.migrator.jdbc.type.JdbcEnumType;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.apache.commons.lang3.StringUtils;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class XmlJdbcEnumTypeHandler<T extends JdbcEnumType> extends XmlJdbcTypeHandler<T> {

    private static final String VALUE_ELEMENT = "value";

    public XmlJdbcEnumTypeHandler() {
        this((Class<? extends T>) JdbcEnumType.class);
    }

    public XmlJdbcEnumTypeHandler(Class<? extends T> type) {
        super(type);
    }

    @Override
    protected void readElement(InputNode input, T jdbcType, XmlReadContext context) throws Exception {
        final String element = input.getName();
        if (VALUE_ELEMENT.equals(element)) {
            jdbcType.addValue(context.read(input, String.class, EMPTY));
        } else {
            super.readElement(input, jdbcType, context);
        }
    }

    @Override
    protected void writeElements(T jdbcType, OutputNode output, XmlWriteContext context) throws Exception {
        super.writeElements(jdbcType, output, context);
        for (String value : jdbcType.getValues()) {
            context.writeElement(output, VALUE_ELEMENT, value);
        }
    }
}
