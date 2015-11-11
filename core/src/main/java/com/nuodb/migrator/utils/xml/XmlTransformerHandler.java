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
package com.nuodb.migrator.utils.xml;

import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;
import org.simpleframework.xml.transform.Matcher;
import org.simpleframework.xml.transform.RegistryMatcher;
import org.simpleframework.xml.transform.Transformer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class XmlTransformerHandler implements XmlReadWriteHandler {

    private Transformer transformer;

    public XmlTransformerHandler() {
        RegistryMatcher matcher = new RegistryMatcher();
        matcher.bind(Boolean.class, BooleanTransform.class);
        matcher.bind(Byte.class, ByteTransform.class);
        matcher.bind(Character.class, CharacterTransform.class);
        matcher.bind(Integer.class, IntegerTransform.class);
        matcher.bind(Long.class, LongTransform.class);
        matcher.bind(Short.class, ShortTransform.class);
        matcher.bind(Float.class, FloatTransform.class);
        matcher.bind(Double.class, DoubleTransform.class);
        matcher.bind(Date.class, DateTransform.class);
        matcher.bind(BigInteger.class, BigIntegerTransform.class);
        matcher.bind(BigDecimal.class, BigDecimalTransform.class);

        this.transformer = new Transformer(matcher);
    }

    public XmlTransformerHandler(Matcher matcher) {
        this(new Transformer(matcher));
    }

    public XmlTransformerHandler(Transformer transformer) {
        this.transformer = transformer;
    }

    @Override
    public Object read(InputNode input, Class type, XmlReadContext context) {
        try {
            return transformer.read(input.getValue(), type);
        } catch (Exception exception) {
            throw new XmlPersisterException(exception);
        }
    }

    @Override
    public boolean canRead(InputNode input, Class type, XmlReadContext context) {
        return canConvert(type);
    }

    @Override
    public boolean skip(Object source, Class type, OutputNode output, XmlWriteContext context) {
        return false;
    }

    @Override
    public boolean write(Object source, Class type, OutputNode output, XmlWriteContext context) {
        try {
            output.setValue(transformer.write(source, type));
            return true;
        } catch (Exception exception) {
            throw new XmlPersisterException(exception);
        }
    }

    @Override
    public boolean canWrite(Object source, Class type, OutputNode output, XmlWriteContext context) {
        return canConvert(type);
    }

    protected boolean canConvert(Class type) {
        try {
            return type != null && transformer.valid(type);
        } catch (Exception exception) {
            throw new XmlPersisterException(exception);
        }
    }
}
