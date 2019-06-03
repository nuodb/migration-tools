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

import com.nuodb.migrator.backup.format.InputBase;
import com.nuodb.migrator.backup.format.InputException;
import com.nuodb.migrator.backup.format.value.Value;
import com.nuodb.migrator.backup.format.value.ValueType;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.io.Reader;
import java.util.BitSet;
import java.util.List;

import static com.nuodb.migrator.backup.format.utils.BinaryEncoder.BASE64;
import static com.nuodb.migrator.backup.format.utils.BitSetUtils.EMPTY;
import static com.nuodb.migrator.backup.format.utils.BitSetUtils.fromHexString;
import static com.nuodb.migrator.backup.format.value.ValueType.STRING;
import static com.nuodb.migrator.backup.format.value.ValueUtils.binary;
import static com.nuodb.migrator.backup.format.value.ValueUtils.string;
import static javax.xml.XMLConstants.NULL_NS_URI;
import static javax.xml.stream.XMLInputFactory.newInstance;

/**
 * @author Sergey Bushik
 */
public class XmlInput extends InputBase implements XmlFormat {

    private XMLStreamReader xmlReader;

    @Override
    public String getFormat() {
        return TYPE;
    }

    @Override
    protected void init(Reader reader) {
        try {
            xmlReader = newInstance().createXMLStreamReader(reader);
        } catch (XMLStreamException exception) {
            throw new InputException(exception);
        }
    }

    @Override
    protected void init(InputStream inputStream) {
        try {
            xmlReader = newInstance().createXMLStreamReader(getInputStream(),
                    (String) getAttribute(ATTRIBUTE_ENCODING, ENCODING));
        } catch (XMLStreamException exception) {
            throw new InputException(exception);
        }
    }

    @Override
    public void readStart() {
        nextElement();
    }

    @Override
    public Value[] readValues() {
        Value[] values = null;
        if (isNextElement(ELEMENT_ROW)) {
            String nullsAttribute = getAttributeValue(NULL_NS_URI, ATTRIBUTE_NULLS);
            BitSet nulls = nullsAttribute != null ? fromHexString(nullsAttribute) : EMPTY;
            List<ValueType> valueTypes = getValueTypes();
            int length = valueTypes.size();
            values = new Value[length];
            int index = 0;
            while (index < length) {
                String value = null;
                ValueType valueType = valueTypes.get(index);
                if (!nulls.get(index) && isNextElement(ELEMENT_COLUMN)) {
                    try {
                        ValueType valueLevel = VALUE_TYPES
                                .fromAlias(getAttributeValue(NULL_NS_URI, ATTRIBUTE_VALUE_TYPE));
                        valueType = valueLevel != null ? valueLevel : valueType;
                        value = xmlReader.getElementText();
                    } catch (XMLStreamException exception) {
                        throw new InputException(exception);
                    }
                }
                valueType = valueType != null ? valueType : STRING;
                switch (valueType) {
                case BINARY:
                    values[index] = binary(BASE64.decode(value));
                    break;
                case STRING:
                    values[index] = string(value);
                    break;
                }
                index++;
            }
        }
        return values;
    }

    protected boolean nextElement() {
        while (xmlReader.getEventType() != XMLStreamConstants.END_DOCUMENT) {
            try {
                switch (xmlReader.next()) {
                case XMLStreamConstants.START_ELEMENT:
                    return true;
                }
            } catch (XMLStreamException exception) {
                throw new InputException(exception);
            }
        }
        return false;
    }

    protected boolean isNextElement(String name) {
        return nextElement() && isCurrentElement(name);
    }

    protected boolean isCurrentElement(String element) {
        return xmlReader.getEventType() == XMLStreamConstants.START_ELEMENT && xmlReader.getLocalName().equals(element);
    }

    protected String getAttributeValue(String namespace, String attribute) {
        for (int index = 0; index < xmlReader.getAttributeCount(); index++) {
            QName name = xmlReader.getAttributeName(index);
            if (name.getNamespaceURI().equals(namespace) && name.getLocalPart().equals(attribute)) {
                return xmlReader.getAttributeValue(index);
            }
        }
        return null;
    }

    @Override
    public void readEnd() {
    }

    @Override
    public void close() {
        if (xmlReader != null) {
            try {
                xmlReader.close();
            } catch (XMLStreamException exception) {
                throw new InputException(exception);
            }
            xmlReader = null;
        }
    }
}
