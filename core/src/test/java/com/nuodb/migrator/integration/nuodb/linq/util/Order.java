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
package com.nuodb.migrator.integration.nuodb.linq.util;

public class Order {
    String orderid;
    String orderdate;
    double total;
    String customerid;

    public Order(String orderid, String orderdate, double total, String customerid) {
        this.orderid = orderid;
        this.orderdate = orderdate;
        this.total = total;
        this.customerid = customerid;
    }

    public String getOrderid() {
        return orderid;
    }

    public String getOrderdate() {
        return orderdate;
    }

    public double getTotal() {
        return total;
    }

    public String getCustomerid() {
        return customerid;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((customerid == null) ? 0 : customerid.hashCode());
        result = prime * result + ((orderdate == null) ? 0 : orderdate.hashCode());
        result = prime * result + ((orderid == null) ? 0 : orderid.hashCode());
        long temp;
        temp = Double.doubleToLongBits(total);
        result = prime * result + (int) (temp ^ (temp >>> 32));
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
        Order other = (Order) obj;
        if (customerid == null) {
            if (other.customerid != null)
                return false;
        } else if (!customerid.equals(other.customerid))
            return false;
        if (orderdate == null) {
            if (other.orderdate != null)
                return false;
        } else if (!orderdate.equals(other.orderdate))
            return false;
        if (orderid == null) {
            if (other.orderid != null)
                return false;
        } else if (!orderid.equals(other.orderid))
            return false;
        if (Double.doubleToLongBits(total) != Double.doubleToLongBits(other.total))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Order [orderid=" + orderid + ", orderdate=" + orderdate + ", total=" + total + ", customerid="
                + customerid + ", getOrderid()=" + getOrderid() + ", getOrderdate()=" + getOrderdate() + ", getTotal()="
                + getTotal() + ", getCustomerid()=" + getCustomerid() + ", hashCode()=" + hashCode() + ", getClass()="
                + getClass() + ", toString()=" + super.toString() + "]";
    }
}