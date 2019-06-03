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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Customer {
    private String companyname;
    private int month;
    private String customerid;
    private String region;
    private List<Order> list1;

    public Customer(String companyname, int month, String customerid, String region, List<Order> list1) {
        this.companyname = companyname;
        this.month = month;
        this.customerid = customerid;
        this.region = region;
        this.list1 = list1;
    }

    public Customer(String customerid) {
        this.customerid = customerid;
        list1 = new ArrayList<Order>();
    }

    public String getCompanyname() {
        return companyname;
    }

    public void setCompanyname(String companyname) {
        this.companyname = companyname;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public void setCustomerid(String customerid) {
        this.customerid = customerid;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public int getMonth() {
        return month;
    }

    public String getCustomerid() {
        return customerid;
    }

    public String getRegion() {
        return region;
    }

    public List<Order> getList1() {
        return list1;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((companyname == null) ? 0 : companyname.hashCode());
        result = prime * result + ((customerid == null) ? 0 : customerid.hashCode());
        result = prime * result + ((list1 == null) ? 0 : list1.hashCode());
        result = prime * result + month;
        result = prime * result + ((region == null) ? 0 : region.hashCode());
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
        Customer other = (Customer) obj;
        if (companyname == null) {
            if (other.companyname != null)
                return false;
        } else if (!companyname.equals(other.companyname))
            return false;
        if (customerid == null) {
            if (other.customerid != null)
                return false;
        } else if (!customerid.equals(other.customerid))
            return false;
        if (list1 == null) {
            if (other.list1 != null)
                return false;
        } else if (!list1.equals(other.list1))
            return false;
        if (month != other.month)
            return false;
        if (region == null) {
            if (other.region != null)
                return false;
        } else if (!region.equals(other.region))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Customer [companyname=" + companyname + ", month=" + month + ", customerid=" + customerid + ", region="
                + region + ", list1=" + list1 + "]";
    }

    public static Comparator<Customer> getSortBasedOnCustomerID() {
        return new Comparator<Customer>() {
            public int compare(Customer c1, Customer c2) {
                if (c1.getCustomerid().compareTo(c2.getCustomerid()) > 0) {
                    return 1;
                } else if (c1.getCustomerid().compareTo(c2.getCustomerid()) < 0) {
                    return -1;
                } else {
                    return 0;
                }
            }
        };
    }
}
