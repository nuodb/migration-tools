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

import com.nuodb.migrator.jdbc.metadata.Index;
import com.nuodb.migrator.jdbc.metadata.SortOrder;
import com.nuodb.migrator.jdbc.metadata.Column;
import com.nuodb.migrator.jdbc.metadata.Table;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;
import org.simpleframework.xml.stream.OutputNode;

import static com.nuodb.migrator.jdbc.metadata.SortOrder.valueOf;
import static com.nuodb.migrator.utils.StringUtils.lowerCase;
import static com.nuodb.migrator.utils.StringUtils.upperCase;

/**
 * @author Sergey Bushik
 */
public class XmlIndexHandler extends XmlIdentifiableHandlerBase<Index> {

    private static final String UNIQUE_ATTRIBUTE = "unique";
    private static final String EXPRESSION_ATTRIBUTE = "expression";
    private static final String SORT_ORDER_ATTRIBUTE = "sort-order";
    private static final String FILTER_CONDITION_ATTRIBUTE = "filter-condition";
    private static final String COLUMN_ELEMENT = "column";
    private static final String INDEX_TYPE = "type";

    public XmlIndexHandler() {
        super(Index.class);
    }

    @Override
    protected void readAttributes(InputNode input, Index target, XmlReadContext context) throws Exception {
        super.readAttributes(input, target, context);
        target.setUnique(context.readAttribute(input, UNIQUE_ATTRIBUTE, boolean.class));
        /* read type property from XML */
        target.setType(context.readAttribute(input, INDEX_TYPE, String.class));
        String sortOrder = context.readAttribute(input, SORT_ORDER_ATTRIBUTE, String.class);
        if (sortOrder != null) {
            target.setSortOrder(valueOf(upperCase(sortOrder)));
        }
        target.setExpression(context.readAttribute(input, EXPRESSION_ATTRIBUTE, String.class));
        target.setFilterCondition(context.readAttribute(input, FILTER_CONDITION_ATTRIBUTE, String.class));
    }

    @Override
    protected void readElement(InputNode input, Index index, XmlReadContext context) throws Exception {
        Table table = getParent(context);
        String element = input.getName();
        if (COLUMN_ELEMENT.equals(element)) {
            index.addColumn(table.getColumn(context.readAttribute(input, NAME_ATTRIBUTE, String.class)),
                    index.getColumns().size());
        }
    }

    @Override
    protected void writeAttributes(Index index, OutputNode output, XmlWriteContext context) throws Exception {
        super.writeAttributes(index, output, context);
        context.writeAttribute(output, UNIQUE_ATTRIBUTE, index.isUnique());
        SortOrder sortOrder = index.getSortOrder();
        /* write type property in XML */
        if (index.getType() != null) {
            context.writeAttribute(output, INDEX_TYPE, upperCase(index.getType()));
        }
        if (sortOrder != null) {
            context.writeAttribute(output, SORT_ORDER_ATTRIBUTE, lowerCase(sortOrder.name()));
        }
        String expression = index.getExpression();
        if (expression != null) {
            context.writeAttribute(output, EXPRESSION_ATTRIBUTE, expression);
        }
        final String filterCondition = index.getFilterCondition();
        if (filterCondition != null) {
            context.writeAttribute(output, FILTER_CONDITION_ATTRIBUTE, filterCondition);
        }
    }

    @Override
    protected void writeElements(Index index, OutputNode output, XmlWriteContext context) throws Exception {
        for (Column column : index.getColumns()) {
            OutputNode element = output.getChild(COLUMN_ELEMENT);
            context.writeAttribute(element, NAME_ATTRIBUTE, column.getName());
        }
    }
}
