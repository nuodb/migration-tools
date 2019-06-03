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
package com.nuodb.migrator.integration.nuodb.linq;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.nuodb.migrator.integration.MigrationTestBase;
import com.nuodb.migrator.integration.nuodb.linq.util.DatabaseUtil;
import com.nuodb.migrator.integration.nuodb.linq.util.Product;

@Test(groups = { "nuodblinqtest" }, dependsOnGroups = { "linqdataloadperformed" })
public class QuantifiersTest extends MigrationTestBase {
    PreparedStatement pstmt = null;

    public void subStringCheck() throws Exception {
        List<String> list = DatabaseUtil.getSimWordsArray(
                "select similarword from array4 where similarword like '%ei%'", nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        Assert.assertTrue(list.contains("receipt"), "No words condain the substring ei ");
    }

    public void outOfStockDetails() throws Exception {
        List<Product> products = DatabaseUtil.getProductList(
                "select * from products where category IN (select category from products where stock =0 group"
                        + " by category)",
                nuodbConnection, pstmt);
        Assert.assertTrue(products.size() >= 1, "The product list is empty");
        List<Product> verifyProdList = getOutOfStockDetails();
        Collections.sort(products, Product.getSortBasedOnProductID());
        Collections.sort(verifyProdList, Product.getSortBasedOnProductID());
        Assert.assertEquals(products.size(), verifyProdList.size(), "Data Mismatch");
        for (int i = 0; i < products.size(); i++) {
            Assert.assertEquals(products.get(i), verifyProdList.get(i),
                    "Object data not matched : Product(" + i + ") " + products.get(i) + " " + verifyProdList.get(i));
        }
    }

    private List<Product> getOutOfStockDetails() {
        List<Product> list = new ArrayList<Product>();
        list.add(new Product(1, "Alice Mutton", 0, "Beverages", "CheapestProducts", 4.5000, 10, 23));
        list.add(new Product(3, "Gorgonzola", 0, "Produce", "CheapestProducts", 25, 3, 10));
        list.add(new Product(4, "Perth Pasties", 0, "Seafood", "CheapestProducts", 38, 5, 5));
        list.add(new Product(5, "Outback Lager", 1, "Beverages", "CheapestProducts", 3.3, 5, 12));
        return list;
    }

    public void oddCheck() throws Exception {
        List<Integer> list = DatabaseUtil.getNumCArray("SELECT numc FROM array2 WHERE cast (numc as int) % 2 = 1",
                nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        Integer[] actarr = list.toArray(new Integer[list.size()]);
        Integer exparr[] = { 1, 11, 3, 19, 41, 65, 19 };
        Assert.assertEqualsNoOrder(actarr, exparr, "Array contains even numbers");

    }

    public void inStockDetails() throws Exception {
        List<Product> products = DatabaseUtil.getProductList("select * from products where stock>0", nuodbConnection,
                pstmt);
        Assert.assertTrue(products.size() >= 1, "The product list is empty");
        List<Product> verifyProdList = getInStockDetails();
        Collections.sort(products, Product.getSortBasedOnProductID());
        Collections.sort(verifyProdList, Product.getSortBasedOnProductID());
        Assert.assertEquals(products.size(), verifyProdList.size(), "Data Mismatch");
        for (int i = 0; i < products.size(); i++) {
            Assert.assertEquals(products.get(i), verifyProdList.get(i),
                    "Object data not matched : Product(" + i + ") " + products.get(i) + " " + verifyProdList.get(i));
        }
    }

    private List<Product> getInStockDetails() {
        List<Product> list = new ArrayList<Product>();
        list.add(new Product(2, "Chef Anton", 1, "Condiments", "CheapestProducts", 7.4500, 4, 12));
        list.add(new Product(5, "Outback Lager", 1, "Beverages", "CheapestProducts", 3.3, 5, 12));
        return list;
    }
}
