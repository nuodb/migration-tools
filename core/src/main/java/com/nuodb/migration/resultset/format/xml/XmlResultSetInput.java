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
package com.nuodb.migration.resultset.format.xml;

import com.google.common.collect.Lists;
import com.nuodb.migration.jdbc.model.ColumnSetModel;
import com.nuodb.migration.jdbc.type.jdbc2.JdbcCharType;
import com.nuodb.migration.resultset.format.ResultSetInputBase;
import com.nuodb.migration.resultset.format.ResultSetInputException;
import org.apache.commons.lang.StringUtils;

import javax.xml.namespace.QName;
import javax.xml.stream.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.nuodb.migration.jdbc.model.ColumnModelFactory.createColumnSetModel;
import static javax.xml.XMLConstants.NULL_NS_URI;
import static javax.xml.XMLConstants.W3C_XML_SCHEMA_INSTANCE_NS_URI;

/**
 * @author Sergey Bushik
 */
public class XmlResultSetInput extends ResultSetInputBase implements XmlAttributes {

    private String encoding;
    private XMLStreamReader reader;
    private Iterator<String[]> iterator;

    @Override
    public String getFormatType() {
        return FORMAT_TYPE;
    }

    @Override
    protected void doInitInput() {
        encoding = getAttribute(ATTRIBUTE_ENCODING, ENCODING);

        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        try {
            if (getReader() != null) {
                reader = xmlInputFactory.createXMLStreamReader(getReader());
            } else if (getInputStream() != null) {
                reader = xmlInputFactory.createXMLStreamReader(getInputStream());
            }
        } catch (XMLStreamException e) {
            throw new ResultSetInputException(e);
        }
        iterator = createIterator();

    }

    protected Iterator<String[]> createIterator() {
        return new XmlInputIterator();
    }

    @Override
    protected void doReadBegin() {
        ColumnSetModel columnSetModel = null;
        if (isNextElement(RESULT_SET_ELEMENT) && isNextElement(COLUMNS_ELEMENT)) {
            List<String> columns = Lists.newArrayList();
            while (isNextElement(COLUMN_ELEMENT)) {
                String column = getAttributeValue(NULL_NS_URI, ATTRIBUTE_NAME);
                if (column != null) {
                    columns.add(column);
                } else {
                    Location location = reader.getLocation();
                    throw new ResultSetInputException(
                            String.format("Element %s doesn't have %s attribute [location at %d:%d]",
                                    COLUMN_ELEMENT, ATTRIBUTE_NAME, location.getLineNumber(),
                                    location.getColumnNumber()));
                }
            }
            int[] columnTypes = new int[columns.size()];
            Arrays.fill(columnTypes, JdbcCharType.INSTANCE.getTypeDesc().getTypeCode());
            columnSetModel = createColumnSetModel(columns.toArray(new String[columns.size()]), columnTypes);
        }
        setColumnSetModel(columnSetModel);
    }

    @Override
    public boolean hasNextRow() {
        return iterator != null && iterator.hasNext();
    }

    @Override
    public void readRow() {
        readRow(iterator.next());
    }

    protected String[] doReadRow() {
        String[] values = null;
        if (isCurrentElement(ROW_ELEMENT) || isNextElement(ROW_ELEMENT)) {
            values = new String[getColumnSetModel().getLength()];
            int column = 0;
            while (isNextElement(COLUMN_ELEMENT)) {
                String nil = getAttributeValue(W3C_XML_SCHEMA_INSTANCE_NS_URI, SCHEMA_NIL_ATTRIBUTE);
                if (!StringUtils.equals(nil, "true")) {
                    try {
                        values[column] = reader.getElementText();
                    } catch (XMLStreamException exception) {
                        throw new ResultSetInputException(exception);
                    }
                }
                column++;
            }
        }
        return values;
    }

    protected boolean isNextElement(String name) {
        while (reader.getEventType() != XMLStreamConstants.END_DOCUMENT) {
            try {
                switch (reader.next()) {
                    case XMLStreamConstants.START_ELEMENT:
                        return reader.getLocalName().equals(name);
                }
            } catch (XMLStreamException exception) {
                throw new ResultSetInputException(exception);
            }
        }
        return false;
    }

    protected boolean isCurrentElement(String element) {
        return reader.getEventType() == XMLStreamConstants.START_ELEMENT &&
                reader.getLocalName().equals(element);
    }

    protected String getAttributeValue(String namespace, String attribute) {
        for (int index = 0; index < reader.getAttributeCount(); index++) {
            QName name = reader.getAttributeName(index);
            if (name.getNamespaceURI().equals(namespace) && name.getLocalPart().equals(attribute)) {
                return reader.getAttributeValue(index);
            }
        }
        return null;
    }

    @Override
    protected void doReadEnd() {
        try {
            reader.close();
        } catch (XMLStreamException exception) {
            throw new ResultSetInputException(exception);
        }
    }

    class XmlInputIterator implements Iterator<String[]> {

        private String[] current;

        @Override
        public boolean hasNext() {
            if (current == null) {
                current = doReadRow();
            }
            return current != null;
        }

        @Override
        public String[] next() {
            String[] next = current;
            current = null;
            if (next == null) {
                // hasNext() wasn't called before
                next = doReadRow();
                if (next == null) {
                    throw new ResultSetInputException("No more rows available");
                }
            }
            return next;
        }

        @Override
        public void remove() {
            throw new ResultSetInputException("Removal is unsupported operation");
        }
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }
}
