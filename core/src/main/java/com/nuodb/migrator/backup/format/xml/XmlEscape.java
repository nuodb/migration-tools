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
package com.nuodb.migrator.backup.format.xml;

import com.google.common.collect.Maps;
import org.apache.xerces.util.XMLChar;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

/**
 * @author Sergey Bushik
 */
public class XmlEscape {

    public static final XmlEscape INSTANCE = new XmlEscape();

    private EntityMap entityMap = new EntityMap();

    public XmlEscape() {
        String[][] entities = { { "quot", "34" }, { "amp", "38" }, { "lt", "60" }, { "gt", "62" }, { "apos", "39" } };
        for (String[] entity : entities) {
            addEntity(entity[0], Integer.parseInt(entity[1]));
        }
    }

    public void addEntity(String name, int value) {
        entityMap.add(name, value);
    }

    public String escape(String text) {
        if (text == null) {
            return null;
        }
        StringWriter writer = new StringWriter(text.length());
        try {
            this.escape(writer, text);
        } catch (IOException exception) {
            throw new XmlEscapeException(exception);
        }
        return writer.toString();
    }

    public String unescape(String text) {
        if (text == null) {
            return null;
        }
        int amp = text.indexOf('&');
        if (amp < 0) {
            return text;
        } else {
            StringWriter writer = new StringWriter(text.length());
            try {
                unescape(writer, text, amp);
            } catch (IOException exception) {
                throw new XmlEscapeException(exception);
            }
            return writer.toString();
        }
    }

    protected String entityName(int value) {
        return entityMap.name(value);
    }

    protected Integer entityValue(String entity) {
        return entityMap.value(entity);
    }

    protected void escape(Writer writer, String text) throws IOException {
        int len = text.length();
        for (int i = 0; i < len; i++) {
            char c = text.charAt(i);
            String entity = entityName(c);
            if (entity == null) {
                if (c > 0x7F || XMLChar.isInvalid(c)) {
                    writer.write("&#");
                    writer.write(Integer.toString(c, 10));
                    writer.write(';');
                } else {
                    writer.write(c);
                }
            } else {
                writer.write('&');
                writer.write(entity);
                writer.write(';');
            }
        }
    }

    protected void unescape(Writer writer, String text, int amp) throws IOException {
        writer.write(text, 0, amp);
        for (int index = amp, count = text.length(); index < count; index++) {
            char c = text.charAt(index);
            if (c == '&') {
                int next = index + 1;
                int semi = text.indexOf(';', next);
                if (semi == -1) {
                    writer.write(c);
                    continue;
                }
                amp = text.indexOf('&', index + 1);
                if (amp != -1 && amp < semi) {
                    writer.write(c);
                    continue;
                }
                String entity = text.substring(next, semi);
                Integer value = null;
                int length = entity.length();
                if (length > 0) {
                    if (entity.charAt(0) == '#') {
                        if (length > 1) {
                            char hex = entity.charAt(1);
                            try {
                                switch (hex) {
                                case 'X':
                                case 'x':
                                    value = Integer.parseInt(entity.substring(2), 16);
                                    break;
                                default:
                                    value = Integer.parseInt(entity.substring(1), 10);
                                }
                                if (value > 0xFFFF) {
                                    value = null;
                                }
                            } catch (NumberFormatException exception) {
                                value = null;
                            }
                        }
                    } else {
                        value = entityValue(entity);
                    }
                }
                if (value == null) {
                    writer.write('&');
                    writer.write(entity);
                    writer.write(';');
                } else {
                    writer.write(value);
                }
                index = semi;
            } else {
                writer.write(c);
            }
        }
    }

    class EntityMap {

        private final Map<String, Integer> names = Maps.newHashMap();
        private final Map<Integer, String> values = Maps.newHashMap();

        public void add(String name, int value) {
            names.put(name, value);
            values.put(value, name);
        }

        public String name(int value) {
            return values.get(value);
        }

        public Integer value(String name) {
            return names.get(name);
        }
    }
}
