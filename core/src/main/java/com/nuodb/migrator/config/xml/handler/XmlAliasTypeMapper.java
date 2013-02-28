/**
 * Copyright (c) 2012, NuoDB, Inc.
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
package com.nuodb.migrator.config.xml.handler;

import com.google.common.collect.Maps;
import com.nuodb.migrator.config.xml.XmlConstants;
import com.nuodb.migrator.config.xml.XmlPersisterException;
import com.nuodb.migrator.config.xml.XmlReadContext;
import com.nuodb.migrator.config.xml.XmlReadHandler;
import org.simpleframework.xml.stream.InputNode;

import java.util.Map;

@SuppressWarnings("unchecked")
public class XmlAliasTypeMapper<T> implements XmlReadHandler<T>, XmlConstants {

    private Map<TypeAlias, Class<? extends T>> typeAliases = Maps.newHashMap();

    public void bind(String namespace, String element, String name, Class<? extends T> type) {
        typeAliases.put(new TypeAlias(namespace, element, name), type);
        typeAliases.put(new TypeAlias(namespace, element, type.getName()), type);
    }

    @Override
    public boolean canRead(InputNode input, Class type, XmlReadContext context) {
        return lookup(input.getReference(), input.getName(), XmlAttributesAccessor.get(input, TYPE_ATTRIBUTE)) != null;
    }

    @Override
    public T read(InputNode input, Class<? extends T> type, XmlReadContext context) {
        String alias = XmlAttributesAccessor.get(input, TYPE_ATTRIBUTE);
        Class<? extends T> definitionType = lookup(input.getReference(), input.getName(), alias);
        if (definitionType == null) {
            throw new XmlPersisterException(String.format("Unable to resolve %1$s name to class", alias));
        }
        return context.read(input, definitionType);
    }

    protected Class<? extends T> lookup(String namespace, String element, String alias) {
        return typeAliases.get(new TypeAlias(namespace, element, alias));
    }

    class TypeAlias {
        private String namespace;
        private String element;
        private String name;

        public TypeAlias(String namespace, String element, String name) {
            this.namespace = namespace;
            this.element = element;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TypeAlias that = (TypeAlias) o;

            if (name != null ? !name.equals(that.name) : that.name != null)
                return false;
            if (element != null ? !element.equals(that.element) : that.element != null) return false;
            if (namespace != null ? !namespace.equals(that.namespace) : that.namespace != null) return false;
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
