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

import com.nuodb.migrator.jdbc.type.JdbcType;
import com.nuodb.migrator.jdbc.type.JdbcTypeDesc;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlReadWriteHandlerBase;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import static com.nuodb.migrator.jdbc.type.JdbcTypeOptions.newOptions;

/**
 * @author Sergey Bushik
 */
public class XmlJdbcTypeHandler<T extends JdbcType> extends XmlReadWriteHandlerBase<T> {

    private static final String CODE_ATTRIBUTE = "code";
    private static final String NAME_ATTRIBUTE = "name";
    private static final String SIZE_ATTRIBUTE = "size";
    private static final String PRECISION_ATTRIBUTE = "precision";
    private static final String SCALE_ATTRIBUTE = "scale";

    public XmlJdbcTypeHandler() {
        super(JdbcType.class);
    }

    public XmlJdbcTypeHandler(Class<? extends T> type) {
        super(type);
    }

    @Override
    protected void readAttributes(InputNode input, T target, XmlReadContext context) throws Exception {
        target.setJdbcTypeDesc(new JdbcTypeDesc(context.readAttribute(input, CODE_ATTRIBUTE, Integer.class),
                context.readAttribute(input, NAME_ATTRIBUTE, String.class)));
        target.setJdbcTypeOptions(newOptions(context.readAttribute(input, SIZE_ATTRIBUTE, Integer.class),
                context.readAttribute(input, PRECISION_ATTRIBUTE, Integer.class),
                context.readAttribute(input, SCALE_ATTRIBUTE, Integer.class)));
    }

    @Override
    protected void writeAttributes(T jdbcType, OutputNode output, XmlWriteContext context) throws Exception {
        context.writeAttribute(output, CODE_ATTRIBUTE, jdbcType.getTypeCode());
        context.writeAttribute(output, NAME_ATTRIBUTE, jdbcType.getTypeName());
        final Long size = jdbcType.getSize();
        if (size != null) {
            context.writeAttribute(output, SIZE_ATTRIBUTE, size);
        }
        final Integer precision = jdbcType.getPrecision();
        if (precision != null) {
            context.writeAttribute(output, PRECISION_ATTRIBUTE, precision);
        }
        final Integer scale = jdbcType.getScale();
        if (scale != null) {
            context.writeAttribute(output, SCALE_ATTRIBUTE, scale);
        }
    }
}
