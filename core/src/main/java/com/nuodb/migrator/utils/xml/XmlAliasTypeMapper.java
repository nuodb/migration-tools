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
 *     * Neither the alias of NuoDB, Inc. nor the names of its contributors may
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

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static com.nuodb.migrator.utils.xml.XmlAttributesAccessor.get;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;

@SuppressWarnings("unchecked")
public class XmlAliasTypeMapper<T> implements XmlReadHandler<T> {

    public static final String TYPE_ATTRIBUTE = "type";

    private String typeAttribute = TYPE_ATTRIBUTE;

    private Map<Alias, Class<? extends T>> aliasTypeMap = newHashMap();

    public void bind(String name, Class<? extends T> type) {
        bind(null, null, name, type);
    }

    public void bind(String namespace, String element, Class<? extends T> type) {
        bind(namespace, element, null, type);
    }

    public void bind(String namespace, String element, String name, Class<? extends T> type) {
        aliasTypeMap.put(new Alias(namespace, element, name), type);
        aliasTypeMap.put(new Alias(namespace, element, type.getName()), type);
    }

    @Override
    public T read(InputNode input, Class<? extends T> type, XmlReadContext context) {
        Class<? extends T> aliasClass = lookupType(input, type);
        if (aliasClass == null) {
            throw new XmlPersisterException(
                    format("Unable to resolve %s name to class", get(input, getTypeAttribute())));
        }
        return context.read(input, aliasClass);
    }

    protected Class<? extends T> lookupType(InputNode input, Class type) {
        Alias alias = new Alias(input.getReference(), input.getName(), get(input, getTypeAttribute()));
        return aliasTypeMap.get(alias);
    }

    @Override
    public boolean canRead(InputNode input, Class type, XmlReadContext context) {
        return lookupType(input, type) != null;
    }

    public String getTypeAttribute() {
        return typeAttribute;
    }

    public void setTypeAttribute(String typeAttribute) {
        this.typeAttribute = typeAttribute;
    }

    static class Alias {
        private String namespace;
        private String element;
        private String name;

        public Alias(String namespace, String element, String name) {
            this.namespace = namespace == null ? EMPTY : namespace;
            this.element = element;
            this.name = name;
        }

        public String getNamespace() {
            return namespace;
        }

        public String getElement() {
            return element;
        }

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Alias that = (Alias) o;

            if (name != null ? !name.equals(that.name) : that.name != null)
                return false;
            if (element != null ? !element.equals(that.element) : that.element != null)
                return false;
            if (namespace != null ? !namespace.equals(that.namespace) : that.namespace != null)
                return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = namespace != null ? namespace.hashCode() : 0;
            result = 31 * result + (element != null ? element.hashCode() : 0);
            result = 31 * result + (name != null ? name.hashCode() : 0);
            return result;
        }
    }
}
