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

import com.nuodb.migrator.backup.format.value.ValueType;
import com.nuodb.migrator.utils.EnumAlias;

import static com.nuodb.migrator.backup.format.value.ValueType.BINARY;
import static com.nuodb.migrator.backup.format.value.ValueType.STRING;

/**
 * @author Sergey Bushik
 */
public interface XmlFormat {
    final String TYPE = "xml";

    final String ATTRIBUTE_ENCODING = "xml.encoding";
    final String ATTRIBUTE_VERSION = "xml.version";

    final String ENCODING = "utf-8";
    final String VERSION = "1.0";

    final String ELEMENT_ROWS = "rs";
    final String ELEMENT_COLUMN = "c";
    final String ELEMENT_ROW = "r";
    final String ATTRIBUTE_NULLS = "ns";
    final String ATTRIBUTE_VALUE_TYPE = "vt";
    final String ATTRIBUTE_VALUE_TYPE_STRING = "s";
    final String ATTRIBUTE_VALUE_TYPE_BINARY = "b";

    final EnumAlias<ValueType> VALUE_TYPES = new EnumAlias<ValueType>() {
        {
            addAlias(ATTRIBUTE_VALUE_TYPE_STRING, STRING);
            addAlias(ATTRIBUTE_VALUE_TYPE_BINARY, BINARY);
        }
    };
}
