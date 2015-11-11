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

import com.nuodb.migrator.backup.Column;
import com.nuodb.migrator.backup.format.OutputBase;
import com.nuodb.migrator.backup.format.OutputException;
import com.nuodb.migrator.backup.format.value.Value;
import com.nuodb.migrator.backup.format.value.ValueType;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.OutputStream;
import java.io.Writer;
import java.util.BitSet;
import java.util.Collection;

import static com.google.common.collect.Iterables.get;
import static com.nuodb.migrator.backup.format.utils.BinaryEncoder.BASE64;
import static com.nuodb.migrator.backup.format.utils.BitSetUtils.toHexString;
import static com.nuodb.migrator.backup.format.value.ValueType.BINARY;
import static com.nuodb.migrator.backup.format.xml.XmlUtils.isValid;
import static javax.xml.stream.XMLOutputFactory.newInstance;

/**
 * @author Sergey Bushik
 */
public class XmlOutput extends OutputBase implements XmlFormat {

    private XMLStreamWriter xmlWriter;

    @Override
    public String getFormat() {
        return TYPE;
    }

    @Override
    protected void init(OutputStream outputStream) {
        try {
            xmlWriter = newInstance().createXMLStreamWriter(outputStream, getEncoding());
        } catch (XMLStreamException exception) {
            throw new OutputException(exception);
        }
    }

    @Override
    protected void init(Writer writer) {
        try {
            xmlWriter = newInstance().createXMLStreamWriter(writer);
        } catch (XMLStreamException exception) {
            throw new OutputException(exception);
        }
    }

    @Override
    public void writeStart() {
        try {
            xmlWriter.writeStartDocument(getEncoding(), getVersion());
            xmlWriter.setPrefix(XMLConstants.DEFAULT_NS_PREFIX, XMLConstants.NULL_NS_URI);
            xmlWriter.writeStartElement(ELEMENT_ROWS);
        } catch (XMLStreamException exception) {
            throw new OutputException(exception);
        }
    }

    @Override
    public void writeValues(Value[] values) {
        try {
            xmlWriter.writeStartElement(ELEMENT_ROW);
            BitSet nulls = new BitSet();
            for (int i = 0; i < values.length; i++) {
                nulls.set(i, values[i].isNull());
            }
            if (!nulls.isEmpty()) {
                xmlWriter.writeAttribute(ATTRIBUTE_NULLS, toHexString(nulls));
            }
            int i = 0;
            Collection<Column> columns = getRowSet().getColumns();
            for (Value value : values) {
                if (!value.isNull()) {
                    ValueType valueType = get(columns, i).getValueType();
                    xmlWriter.writeStartElement(ELEMENT_COLUMN);
                    String content;
                    if (valueType == BINARY) {
                        content = BASE64.encode(value.asBytes());
                    } else if (!isValid(value.asString())) {
                        xmlWriter.writeAttribute(ATTRIBUTE_VALUE_TYPE, VALUE_TYPES.toAlias(BINARY));
                        content = BASE64.encode(value.asBytes());
                    } else {
                        content = value.asString();
                    }
                    xmlWriter.writeCharacters(content);
                    xmlWriter.writeEndElement();
                }
                i++;
            }
            xmlWriter.writeEndElement();
        } catch (XMLStreamException e) {
            throw new OutputException(e);
        }
    }

    @Override
    public void writeEnd() {
        try {
            xmlWriter.writeEndElement();
            xmlWriter.writeEndDocument();
            xmlWriter.flush();
        } catch (XMLStreamException exception) {
            throw new OutputException(exception);
        }
    }

    @Override
    public void close() {
        if (xmlWriter != null) {
            try {
                xmlWriter.close();
            } catch (XMLStreamException exception) {
                throw new OutputException(exception);
            }
            xmlWriter = null;
        }
    }

    protected String getEncoding() {
        return (String) getAttribute(ATTRIBUTE_VERSION, ENCODING);
    }

    protected String getVersion() {
        return (String) getAttribute(ATTRIBUTE_ENCODING, VERSION);
    }
}
