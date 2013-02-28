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
package com.nuodb.migrator.resultset.format.value;

import java.util.Arrays;

/**
 * @author Sergey Bushik
 */
public class ValueVariants {

    public static final ValueVariant BINARY_NULL = new BinaryVariant(null);

    public static final ValueVariant STRING_NULL = new StringVariant(null);

    public static ValueVariant binary(byte[] value) {
        return value == null ? BINARY_NULL : new BinaryVariant(value);
    }

    public static ValueVariant string(String value) {
        return value == null ? STRING_NULL : new StringVariant(value);
    }

    static class BinaryVariant implements ValueVariant {

        private final byte[] value;

        public BinaryVariant(byte[] value) {
            this.value = value;
        }

        @Override
        public String asString() {
            return value != null ? new String(value) : null;
        }

        @Override
        public byte[] asBytes() {
            return value;
        }

        @Override
        public boolean isNull() {
            return value == null;
        }

        @Override
        public ValueVariantType getValueVariantType() {
            return ValueVariantType.BINARY;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BinaryVariant that = (BinaryVariant) o;

            if (!Arrays.equals(value, that.value)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return value != null ? Arrays.hashCode(value) : 0;
        }

        @Override
        public String toString() {
            return "BinaryVariant{" + value + '}';
        }
    }

    static class StringVariant implements ValueVariant {

        public String value;

        public StringVariant(String value) {
            this.value = value;
        }

        @Override
        public String asString() {
            return value;
        }

        @Override
        public byte[] asBytes() {
            return value != null ? value.getBytes() : null;
        }

        @Override
        public boolean isNull() {
            return value == null;
        }

        @Override
        public ValueVariantType getValueVariantType() {
            return ValueVariantType.STRING;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StringVariant that = (StringVariant) o;

            if (value != null ? !value.equals(that.value) : that.value != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "StringVariant{'" + value + "'}";
        }
    }
}
