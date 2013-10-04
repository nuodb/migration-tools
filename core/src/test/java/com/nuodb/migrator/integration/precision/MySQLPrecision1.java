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
package com.nuodb.migrator.integration.precision;


/**
 * Test to make sure all the Tables, Constraints, Views, Triggers etc have been migrated.
 *
 * @author Krishnamoorthy Dhandapani
 */

public class MySQLPrecision1 {
    int t1;
    int t2;
    long t3;
    long t4;
    long t5;

    public MySQLPrecision1(int t1, int t2, long t3, long t4, long t5) {
        this.t1 = t1;
        this.t2 = t2;
        this.t3 = t3;
        this.t4 = t4;
        this.t5 = t5;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + t1;
        result = prime * result + t2;
        result = prime * result + (int) (t3 ^ (t3 >>> 32));
        result = prime * result + (int) (t4 ^ (t4 >>> 32));
        result = prime * result + (int) (t5 ^ (t5 >>> 32));
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
        MySQLPrecision1 other = (MySQLPrecision1) obj;
        if (t1 != other.t1)
            return false;
        if (t2 != other.t2)
            return false;
        if (t3 != other.t3)
            return false;
        if (t4 != other.t4)
            return false;
        if (t5 != other.t5)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "MySqlDataPrecision1 [t1=" + t1 + ", t2=" + t2 + ", t3=" + t3
                + ", t4=" + t4 + ", t5=" + t5 + "]";
    }


}
