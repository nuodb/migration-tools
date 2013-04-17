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

import com.nuodb.migrator.resultset.format.FormatOutputBase;
import com.nuodb.migrator.resultset.format.FormatOutputException;
import com.nuodb.migrator.resultset.format.value.ValueFormatModel;
import com.nuodb.migrator.resultset.format.value.ValueVariant;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.OutputStream;
import java.io.Writer;
import java.util.BitSet;

import static com.nuodb.migrator.resultset.format.utils.BinaryEncoder.BASE64;
import static com.nuodb.migrator.resultset.format.utils.BitSetUtils.toHexString;
import static com.nuodb.migrator.resultset.format.value.ValueVariantType.toAlias;
import static javax.xml.XMLConstants.DEFAULT_NS_PREFIX;
import static javax.xml.XMLConstants.NULL_NS_URI;

/**
 * @author Sergey Bushik
 */
public class XmlOutputFormat extends FormatOutputBase implements XmlAttributes {

    private String encoding;
    private String version;

    private XMLStreamWriter xmlStreamWriter;

    @Override
    public String getType() {
        return FORMAT_TYPE;
    }

    @Override
    public void initOutput() {
        version = (String) getAttribute(ATTRIBUTE_VERSION, VERSION);
        encoding = (String) getAttribute(ATTRIBUTE_ENCODING, ENCODING);

        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        try {
            Writer writer = getWriter();
            OutputStream outputStream = getOutputStream();
            if (writer != null) {
                xmlStreamWriter = factory.createXMLStreamWriter(wrapWriter(writer));
            } else if (outputStream != null) {
                xmlStreamWriter = factory.createXMLStreamWriter(wrapOutputStream(outputStream), getEncoding());
            }
        } catch (XMLStreamException exception) {
            throw new FormatOutputException(exception);
        }
    }

    @Override
    protected void doWriteBegin() {
        try {
            xmlStreamWriter.writeStartDocument(getEncoding(), getVersion());
            xmlStreamWriter.setPrefix(DEFAULT_NS_PREFIX, NULL_NS_URI);
            xmlStreamWriter.writeStartElement(ELEMENT_RESULT_SET);
            xmlStreamWriter.writeStartElement(ELEMENT_COLUMNS);
            for (ValueFormatModel valueFormatModel : getValueFormatModelList()) {
                xmlStreamWriter.writeEmptyElement(ELEMENT_COLUMN);
                xmlStreamWriter.writeAttribute(ATTRIBUTE_NAME, valueFormatModel.getName());
                xmlStreamWriter.writeAttribute(ATTRIBUTE_VARIANT, toAlias(valueFormatModel.getValueVariantType()));
            }
            xmlStreamWriter.writeEndElement();
        } catch (XMLStreamException e) {
            throw new FormatOutputException(e);
        }
    }

    @Override
    protected void writeValues(ValueVariant[] variants) {
        try {
            xmlStreamWriter.writeStartElement(ELEMENT_ROW);
            BitSet nulls = new BitSet();
            for (int i = 0; i < variants.length; i++) {
                nulls.set(i, variants[i].isNull());
            }
            if (!nulls.isEmpty()) {
                xmlStreamWriter.writeAttribute(ATTRIBUTE_NULLS, toHexString(nulls));
            }
            int i = 0;
            for (ValueVariant variant : variants) {
                if (!variant.isNull()) {
                    xmlStreamWriter.writeStartElement(ELEMENT_COLUMN);
                    String value = null;
                    switch (getValueFormatModelList().get(i).getValueVariantType()) {
                        case BINARY:
                            value = BASE64.encode(variant.asBytes());
                            break;
                        case STRING:
                            value = variant.asString();
                            break;
                    }
                    xmlStreamWriter.writeCharacters(value);
                    xmlStreamWriter.writeEndElement();
                }
                i++;
            }
            xmlStreamWriter.writeEndElement();
        } catch (XMLStreamException e) {
            throw new FormatOutputException(e);
        }
    }

    @Override
    protected void doWriteEnd() {
        try {
            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.writeEndDocument();
            xmlStreamWriter.flush();
            xmlStreamWriter.close();
        } catch (XMLStreamException e) {
            throw new FormatOutputException(e);
        }
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
