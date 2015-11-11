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

import com.nuodb.migrator.jdbc.metadata.MetaData;
import com.nuodb.migrator.jdbc.metadata.MetaDataType;
import com.nuodb.migrator.jdbc.metadata.inspector.InspectionScope;
import com.nuodb.migrator.spec.MetaDataSpec;
import com.nuodb.migrator.utils.xml.XmlReadContext;
import com.nuodb.migrator.utils.xml.XmlReadWriteHandlerBase;
import com.nuodb.migrator.utils.xml.XmlWriteContext;
import org.simpleframework.xml.stream.InputNode;

import java.util.Collection;

import static com.nuodb.migrator.spec.MetaDataSpec.OBJECT_TYPES;
import static com.nuodb.migrator.utils.ReflectionUtils.getClassName;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("unchecked")
public class XmlMetaDataHandlerBase<I extends MetaData> extends XmlReadWriteHandlerBase<I> {

    public static final String META_DATA_SPEC = getClassName(MetaDataSpec.class);
    public static final String INSPECTION_SCOPE = getClassName(InspectionScope.class);

    protected XmlMetaDataHandlerBase(Class type) {
        super(type);
    }

    @Override
    public I read(InputNode input, Class<? extends I> type, XmlReadContext context) {
        return super.read(input, type, context);
    }

    @Override
    protected boolean skip(I source, XmlWriteContext context) {
        return skip(source, getMetaDataSpec(context));
    }

    protected boolean skip(MetaDataType objectType, XmlWriteContext context) {
        return skip(objectType, getMetaDataSpec(context));
    }

    protected boolean skip(I source, MetaDataSpec metaDataSpec) {
        return skip(source.getObjectType(), metaDataSpec);
    }

    protected boolean skip(MetaDataType objectType, MetaDataSpec metaDataSpec) {
        Collection<MetaDataType> objectTypes = metaDataSpec != null ? metaDataSpec.getObjectTypes() : OBJECT_TYPES;
        return objectType != null && !objectTypes.contains(objectType);
    }

    protected <I extends InspectionScope> I getInspectionScope(XmlWriteContext context) {
        return (I) context.get(INSPECTION_SCOPE);
    }

    protected MetaDataSpec getMetaDataSpec(XmlWriteContext context) {
        return (MetaDataSpec) context.get(META_DATA_SPEC);
    }
}
