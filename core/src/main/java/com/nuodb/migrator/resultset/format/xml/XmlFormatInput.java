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
package com.nuodb.migrator.resultset.format.xml;

import com.nuodb.migrator.jdbc.model.ValueModelList;
import com.nuodb.migrator.resultset.format.FormatInputBase;
import com.nuodb.migrator.resultset.format.FormatInputException;
import com.nuodb.migrator.resultset.format.value.SimpleValueFormatModel;
import com.nuodb.migrator.resultset.format.value.ValueFormatModel;
import com.nuodb.migrator.resultset.format.value.ValueVariant;
import com.nuodb.migrator.resultset.format.value.ValueVariantType;

import javax.xml.namespace.QName;
import javax.xml.stream.*;
import java.io.Reader;
import java.util.BitSet;
import java.util.Iterator;

import static com.nuodb.migrator.jdbc.model.ValueModelFactory.createValueModelList;
import static com.nuodb.migrator.resultset.format.utils.BinaryEncoder.BASE64;
import static com.nuodb.migrator.resultset.format.utils.BitSetUtils.EMPTY;
import static com.nuodb.migrator.resultset.format.utils.BitSetUtils.fromHexString;
import static com.nuodb.migrator.resultset.format.value.ValueVariantType.STRING;
import static com.nuodb.migrator.resultset.format.value.ValueVariantType.fromAlias;
import static com.nuodb.migrator.resultset.format.value.ValueVariants.binary;
import static com.nuodb.migrator.resultset.format.value.ValueVariants.string;
import static java.lang.String.format;
import static javax.xml.XMLConstants.NULL_NS_URI;

/**
 * @author Sergey Bushik
 */
public class XmlFormatInput extends FormatInputBase implements XmlAttributes {

    private XMLStreamReader xmlStreamReader;
    private Iterator<ValueVariant[]> iterator;

    @Override
    public String getType() {
        return FORMAT_TYPE;
    }

    @Override
    public void initInput() {
        String encoding = (String) getAttribute(ATTRIBUTE_ENCODING, ENCODING);

        XMLInputFactory factory = XMLInputFactory.newInstance();
        try {
            Reader reader = getReader();
            if (reader != null) {
                xmlStreamReader = factory.createXMLStreamReader(reader);
            } else if (getInputStream() != null) {
                xmlStreamReader = factory.createXMLStreamReader(getInputStream(), encoding);
            }
        } catch (XMLStreamException e) {
            throw new FormatInputException(e);
        }
        iterator = createInputIterator();

    }

    protected Iterator<ValueVariant[]> createInputIterator() {
        return new XmlInputIterator();
    }

    @Override
    protected void doReadBegin() {
        ValueModelList<ValueFormatModel> valueFormatModelList = createValueModelList();
        if (isNextElement(ELEMENT_RESULT_SET) && isNextElement(ELEMENT_COLUMNS)) {
            while (isNextElement(ELEMENT_COLUMN)) {
                String column = getAttributeValue(NULL_NS_URI, ATTRIBUTE_NAME);
                ValueFormatModel valueFormatModel = new SimpleValueFormatModel();
                valueFormatModel.setName(column);
                valueFormatModel.setValueVariantType(fromAlias(getAttributeValue(NULL_NS_URI, ATTRIBUTE_VARIANT)));
                if (column == null) {
                    Location location = xmlStreamReader.getLocation();
                    throw new FormatInputException(
                            format("Element %s doesn't have %s attribute [location at %d:%d]",
                                    ELEMENT_COLUMN, ATTRIBUTE_NAME,
                                    location.getLineNumber(), location.getColumnNumber()));
                }
                valueFormatModelList.add(valueFormatModel);
            }
        }
        setValueFormatModelList(valueFormatModelList);
    }

    @Override
    public boolean hasNextRow() {
        return iterator != null && iterator.hasNext();
    }

    @Override
    protected ValueVariant[] readValues() {
        return iterator.next();
    }

    protected ValueVariant[] doReadValues() {
        ValueVariant[] values = null;
        if (isCurrentElement(ELEMENT_ROW)) {
            String nullsValue = getAttributeValue(NULL_NS_URI, ATTRIBUTE_NULLS);
            BitSet nulls = nullsValue != null ? fromHexString(nullsValue) : EMPTY;
            ValueModelList<ValueFormatModel> model = getValueFormatModelList();
            int length = model.size();
            values = new ValueVariant[length];
            int index = 0;
            while (index < length) {
                String value = null;
                if (!nulls.get(index) && isNextElement(ELEMENT_COLUMN)) {
                    try {
                        value = xmlStreamReader.getElementText();
                    } catch (XMLStreamException exception) {
                        throw new FormatInputException(exception);
                    }
                }
                ValueVariantType valueType = model.get(index).getValueVariantType();
                valueType = valueType != null ? valueType : STRING;
                switch (valueType) {
                    case BYTES:
                        values[index] = binary(BASE64.decode(value));
                        break;
                    case STRING:
                        values[index] = string(value);
                        break;
                }
                index++;
            }
            nextElement();
        }
        return values;
    }

    protected boolean nextElement() {
        while (xmlStreamReader.getEventType() != XMLStreamConstants.END_DOCUMENT) {
            try {
                switch (xmlStreamReader.next()) {
                    case XMLStreamConstants.START_ELEMENT:
                        return true;
                }
            } catch (XMLStreamException exception) {
                throw new FormatInputException(exception);
            }
        }
        return false;
    }

    protected boolean isNextElement(String name) {
        return nextElement() && isCurrentElement(name);
    }

    protected boolean isCurrentElement(String element) {
        return xmlStreamReader.getEventType() == XMLStreamConstants.START_ELEMENT &&
                xmlStreamReader.getLocalName().equals(element);
    }

    protected String getAttributeValue(String namespace, String attribute) {
        for (int index = 0; index < xmlStreamReader.getAttributeCount(); index++) {
            QName name = xmlStreamReader.getAttributeName(index);
            if (name.getNamespaceURI().equals(namespace) && name.getLocalPart().equals(attribute)) {
                return xmlStreamReader.getAttributeValue(index);
            }
        }
        return null;
    }

    @Override
    protected void doReadEnd() {
        try {
            xmlStreamReader.close();
        } catch (XMLStreamException exception) {
            throw new FormatInputException(exception);
        }
    }

    class XmlInputIterator implements Iterator<ValueVariant[]> {

        private ValueVariant[] current;

        @Override
        public boolean hasNext() {
            if (current == null) {
                current = doReadValues();
            }
            return current != null;
        }

        @Override
        public ValueVariant[] next() {
            ValueVariant[] next = current;
            current = null;
            if (next == null) {
                next = readValues();
                if (next == null) {
                    throw new FormatInputException("No more rows available");
                }
            }
            return next;
        }

        @Override
        public void remove() {
            throw new FormatInputException("Removal is unsupported operation");
        }
    }
}