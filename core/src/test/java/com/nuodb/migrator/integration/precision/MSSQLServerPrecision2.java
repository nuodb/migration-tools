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

public class MSSQLServerPrecision2 {
    String t1;
    String t2;
    double t3;
    String t4;
    int t5;
    double t6;
    double t7;
    String t8;

    public MSSQLServerPrecision2(String t1, String t2, double t3, String t4, int t5, double t6, double t7, String t8) {
        this.t1 = t1;
        this.t2 = t2;
        this.t3 = t3;
        this.t4 = t4;
        this.t5 = t5;
        this.t6 = t6;
        this.t7 = t7;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((t1 == null) ? 0 : t1.hashCode());
        result = prime * result + ((t2 == null) ? 0 : t2.hashCode());
        long temp;
        temp = Double.doubleToLongBits(t3);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + ((t4 == null) ? 0 : t4.hashCode());
        result = prime * result + t5;
        temp = Double.doubleToLongBits(t6);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(t7);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + ((t8 == null) ? 0 : t8.hashCode());
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
        MSSQLServerPrecision2 other = (MSSQLServerPrecision2) obj;
        if (t1 == null) {
            if (other.t1 != null)
                return false;
        } else if (!t1.equals(other.t1))
            return false;
        if (t2 == null) {
            if (other.t2 != null)
                return false;
        } else if (!t2.equals(other.t2))
            return false;
        if (Double.doubleToLongBits(t3) != Double.doubleToLongBits(other.t3))
            return false;
        if (t4 == null) {
            if (other.t4 != null)
                return false;
        } else if (!t4.equals(other.t4))
            return false;
        if (t5 != other.t5)
            return false;
        if (Double.doubleToLongBits(t6) != Double.doubleToLongBits(other.t6))
            return false;
        if (Double.doubleToLongBits(t7) != Double.doubleToLongBits(other.t7))
            return false;
        if (t8 == null) {
            if (other.t8 != null)
                return false;
        } else if (!t8.equals(other.t8))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "MSSQLServerPrecision2 [t1=" + t1 + ", t2=" + t2 + ", t3=" + t3 + ", t4=" + t4 + ", t5=" + t5 + ", t6="
                + t6 + ", t7=" + t7 + ", t8=" + t8 + "]";
    }

}
