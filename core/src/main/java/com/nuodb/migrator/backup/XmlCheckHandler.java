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
package com.nuodb.migrator.backup;

import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Check;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

/**
 * @author Sergey Bushik
 */
public class XmlCheckHandler extends XmlIdentifiableHandlerBase<Check> {

    private static final String COLUMN_ELEMENT = "column";
    private static final String TEXT_ELEMENT = "text";

    public XmlCheckHandler() {
        super(Check.class);
    }

    @Override
    protected void readElement(InputNode input, Check check, XmlReadContext context) throws Exception {
        String element = input.getName();
        if (COLUMN_ELEMENT.equals(element)) {
            // We get the parent one level up by default.
            // for CHECK, the table is the grandparent.
            Table table = getParent(context, 2);
            check.addColumn(table.getColumn(context.readAttribute(input, NAME_ATTRIBUTE, String.class)));
        } else if (TEXT_ELEMENT.equals(element)) {
            check.setText(context.read(input, String.class));
        }
    }

    @Override
    protected void writeElements(Check check, OutputNode output, XmlWriteContext context) throws Exception {
        context.writeElement(output, TEXT_ELEMENT, check.getText());
        for (Column column : check.getColumns()) {
            OutputNode element = output.getChild(COLUMN_ELEMENT);
            context.writeAttribute(element, NAME_ATTRIBUTE, column.getName());
        }
    }
}
