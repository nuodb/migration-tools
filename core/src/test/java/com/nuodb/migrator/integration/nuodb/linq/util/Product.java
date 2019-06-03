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

import java.util.Comparator;

public class Product {
    private int pid;
    private String pname;
    private int stock;
    private String category;
    private String cheapestproduct;
    private double unitprice;
    private int productcount;
    private int unitinstock;

    public Product(int pid, String pname, int stock, String category, String cheapestproduct, double unitprice,
            int productcount, int unitinstock) {
        this.pid = pid;
        this.pname = pname;
        this.stock = stock;
        this.category = category;
        this.cheapestproduct = cheapestproduct;
        this.unitprice = unitprice;
        this.productcount = productcount;
        this.unitinstock = unitinstock;
    }

    public int getPid() {
        return pid;
    }

    public String getPname() {
        return pname;
    }

    public int getStock() {
        return stock;
    }

    public String getCategory() {
        return category;
    }

    public String getCheapestproduct() {
        return cheapestproduct;
    }

    public double getUnitprice() {
        return unitprice;
    }

    public int getProductcount() {
        return productcount;
    }

    public int getUnitinstock() {
        return unitinstock;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((category == null) ? 0 : category.hashCode());
        result = prime * result + ((cheapestproduct == null) ? 0 : cheapestproduct.hashCode());
        result = prime * result + pid;
        result = prime * result + ((pname == null) ? 0 : pname.hashCode());
        result = prime * result + productcount;
        result = prime * result + stock;
        result = prime * result + unitinstock;
        long temp;
        temp = Double.doubleToLongBits(unitprice);
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
        Product other = (Product) obj;
        if (category == null) {
            if (other.category != null)
                return false;
        } else if (!category.equals(other.category))
            return false;
        if (cheapestproduct == null) {
            if (other.cheapestproduct != null)
                return false;
        } else if (!cheapestproduct.equals(other.cheapestproduct))
            return false;
        if (pid != other.pid)
            return false;
        if (pname == null) {
            if (other.pname != null)
                return false;
        } else if (!pname.equals(other.pname))
            return false;
        if (productcount != other.productcount)
            return false;
        if (stock != other.stock)
            return false;
        if (unitinstock != other.unitinstock)
            return false;
        if (Double.doubleToLongBits(unitprice) != Double.doubleToLongBits(other.unitprice))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Product [pid=" + pid + ", pname=" + pname + ", stock=" + stock + ", category=" + category
                + ", cheapestproduct=" + cheapestproduct + ", unitprice=" + unitprice + ", productcount=" + productcount
                + ", unitinstock=" + unitinstock + ", getPid()=" + getPid() + ", getPname()=" + getPname()
                + ", getStock()=" + getStock() + ", getCategory()=" + getCategory() + ", getCheapestproduct()="
                + getCheapestproduct() + ", getUnitprice()=" + getUnitprice() + ", getProductcount()="
                + getProductcount() + ", getUnitinstock()=" + getUnitinstock() + ", hashCode()=" + hashCode()
                + ", getClass()=" + getClass() + ", toString()=" + super.toString() + "]";
    }

    public static Comparator<Product> getSortBasedOnProductName() {
        return new Comparator<Product>() {
            public int compare(Product p1, Product p2) {
                return p1.getPname().compareTo(p2.getPname());
            }
        };
    }

    public static Comparator<Product> getSortBasedOnProductID() {
        return new Comparator<Product>() {
            public int compare(Product p1, Product p2) {
                if (p1.getPid() > p2.getPid()) {
                    return 1;
                } else if (p1.getPid() < p2.getPid()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        };
    }

}
