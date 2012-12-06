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
package com.nuodb.migration.jdbc.metadata;


import com.nuodb.migration.jdbc.dialect.Dialect;

/**
 * @author Sergey Bushik
 */
public class HasIdentifierBase extends IndentedOutputBase implements HasIdentifier {

    private Identifier identifier;

    public HasIdentifierBase() {
    }

    public HasIdentifierBase(String name) {
        this.identifier = Identifier.valueOf(name);
    }

    public HasIdentifierBase(Identifier identifier) {
        this.identifier = identifier;
    }

    @Override
    public String getName() {
        return identifier != null ? identifier.value() : null;
    }

    @Override
    public void setName(String identifier) {
        this.identifier = Identifier.valueOf(identifier);
    }

    @Override
    public Identifier getIdentifier() {
        return identifier;
    }

    @Override
    public void setIdentifier(Identifier identifier) {
        this.identifier = identifier;
    }

    @Override
    public String getQuotedName(Dialect dialect) {
        return dialect != null ? dialect.getIdentifier(getName()) : getName();
    }

    @Override
    public int compareTo(HasIdentifier hasIdentifier) {
        return identifier.compareTo(hasIdentifier.getIdentifier());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (!(object instanceof HasIdentifier)) return false;
        HasIdentifier hasIdentifier = (HasIdentifier) object;
        if (identifier != null ? !identifier.equals(
                hasIdentifier.getIdentifier()) : hasIdentifier.getIdentifier() != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return identifier != null ? identifier.hashCode() : 0;
    }

    @Override
    public void output(int indent, StringBuilder buffer) {
        output(indent, buffer, identifier != null ? identifier.value() : "");
    }
}
