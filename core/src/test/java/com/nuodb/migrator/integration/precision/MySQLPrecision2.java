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
package com.nuodb.migrator.integration.precision;

/**
 * Test to make sure all the Tables, Constraints, Views, Triggers etc have been
 * migrated.
 * 
 * @author Krishnamoorthy Dhandapani
 */

public class MySQLPrecision2 {
    String varchar;
    String text;
    double decimal;
    double floatType;
    double doubleType;
    String bit;
    String charType;

    public MySQLPrecision2(String varchar, String text, double decimal, double floatType, double doubleType, String bit,
            String charType) {
        this.varchar = varchar;
        this.text = text;
        this.decimal = decimal;
        this.floatType = floatType;
        this.doubleType = doubleType;
        this.bit = bit;
        this.charType = charType;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bit == null) ? 0 : bit.hashCode());
        result = prime * result + ((charType == null) ? 0 : charType.hashCode());
        long temp;
        temp = Double.doubleToLongBits(decimal);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(doubleType);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(floatType);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + ((text == null) ? 0 : text.hashCode());
        result = prime * result + ((varchar == null) ? 0 : varchar.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MySQLPrecision2 other = (MySQLPrecision2) obj;
        if (bit == null) {
            if (other.bit != null)
                return false;
        } else if (!bit.equals(other.bit))
            return false;
        if (charType == null) {
            if (other.charType != null)
                return false;
        } else if (!charType.equals(other.charType))
            return false;
        if (Double.doubleToLongBits(decimal) != Double.doubleToLongBits(other.decimal))
            return false;
        if (Double.doubleToLongBits(doubleType) != Double.doubleToLongBits(other.doubleType))
            return false;
        if (Double.doubleToLongBits(floatType) != Double.doubleToLongBits(other.floatType))
            return false;
        if (text == null) {
            if (other.text != null)
                return false;
        } else if (!text.equals(other.text))
            return false;
        if (varchar == null) {
            if (other.varchar != null)
                return false;
        } else if (!varchar.equals(other.varchar))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "MySQLPrecision2 [varchar=" + varchar + ", text=" + text + ", decimal=" + decimal + ", floatType="
                + floatType + ", doubleType=" + doubleType + ", bit=" + bit + ", charType=" + charType + "]";
    }

}
