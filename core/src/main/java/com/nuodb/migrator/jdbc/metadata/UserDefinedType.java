/**
 * Copyright (c) 2015, NuoDB, Inc.
 * All rights reserved.
 * <p/>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p/>
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of NuoDB, Inc. nor the names of its contributors may
 * be used to endorse or promote products derived from this software
 * without specific prior written permission.
 * <p/>
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
package com.nuodb.migrator.jdbc.metadata;

import static com.nuodb.migrator.jdbc.metadata.MetaDataType.USER_DEFINED_TYPE;
import static java.lang.String.format;

/**
 * @author Mukund
 */
public class UserDefinedType extends IdentifiableBase {

    private static final String COLLECTION = "COLLECTION";
    private static final String ARRAY = "ARRAY";
    private static final String OBJECT = "OBJECT";
    private static final String STRUCT = "STRUCT";

    private Schema schema;
    private String code;

    public UserDefinedType() {
        super(USER_DEFINED_TYPE, true);
    }

    public UserDefinedType(String name) {
        super(USER_DEFINED_TYPE, name, true);
    }

    public UserDefinedType(Identifier identifier) {
        super(USER_DEFINED_TYPE, identifier, true);
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
        if (code != null) {
            if (code.equalsIgnoreCase(COLLECTION)) {
                this.code = ARRAY;
            } else if (code.equalsIgnoreCase(OBJECT)) {
                this.code = STRUCT;
            }
        }
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        super.output(indent, buffer);

        String code = getCode();
        if (code != null) {
            buffer.append(' ');
            buffer.append(format("code=%s", code));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;

        UserDefinedType that = (UserDefinedType) o;

        if (schema != null ? !schema.equals(that.schema) : that.schema != null)
            return false;
        return !(code != null ? !code.equals(that.code) : that.code != null);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (schema != null ? schema.hashCode() : 0);
        result = 31 * result + (code != null ? code.hashCode() : 0);
        return result;
    }
}
