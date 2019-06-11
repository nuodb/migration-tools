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

import org.simpleframework.xml.strategy.Strategy;
import org.simpleframework.xml.strategy.Value;
import org.simpleframework.xml.stream.InputNode;

import java.util.Map;

import static java.lang.String.format;

@SuppressWarnings("unchecked")
public class XmlReadStrategyContext extends XmlReadContextBase {

    private Strategy strategy;

    public XmlReadStrategyContext(Strategy strategy) {
        this.strategy = strategy;
    }

    public XmlReadStrategyContext(Map map, Strategy strategy) {
        super(map);
        this.strategy = strategy;
    }

    @Override
    public <T> T read(InputNode input, Class<T> type, XmlReadContext delegate) {
        Value value;
        try {
            value = strategy.read(new ClassType(type), input.getAttributes(), delegate);
        } catch (XmlPersisterException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new XmlPersisterException(exception);
        }
        if (value != null && value.isReference()) {
            return (T) value.getValue();
        } else {
            throw new XmlPersisterException(format("Failed reading %s from %s", type, input));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        XmlReadStrategyContext that = (XmlReadStrategyContext) o;

        if (strategy != null ? !strategy.equals(that.strategy) : that.strategy != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (strategy != null ? strategy.hashCode() : 0);
        return result;
    }
}
