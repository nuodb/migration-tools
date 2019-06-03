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
public class OrderingTest extends MigrationTestBase {
    PreparedStatement pstmt = null;

    public void orderByAsc() throws Exception {
        List<String> list = DatabaseUtil.getWordsNumArray("select digits from array order by digits asc",
                nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        String[] words = list.toArray(new String[list.size()]);
        String s = null;
        for (String str : words) {
            if (s != null) {
                Assert.assertTrue(((str.compareTo(s) >= 0)), "Not sorted this two strings " + s + " : " + str);
            }
            s = str;
        }
    }

    public void orderByLength() throws Exception {
        List<String> list = DatabaseUtil.getWordsNumArray("select digits from array order by char_length(digits)",
                nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        String s = null;
        for (String str : list) {
            if (s != null) {
                Assert.assertTrue(((s.length() <= str.length())),
                        "Not sorted this two strings with length " + s + " : " + str);
            }
            s = str;
        }
    }

    public void orderByProductNames() throws Exception {
        List<Product> products = DatabaseUtil.getProductList("select * from products order by pname", nuodbConnection,
                pstmt);
        Assert.assertTrue(products.size() >= 1, "The product list is empty");
        List<Product> verifyProduct = getProductDetails();
        Collections.sort(verifyProduct, Product.getSortBasedOnProductName());
        Assert.assertEquals(products.size(), verifyProduct.size(), "Data Mismatch");
        for (int i = 0; i < products.size(); i++) {
            Assert.assertEquals(products.get(i), verifyProduct.get(i),
                    "Object data not matched : Product(" + i + ") " + products.get(i) + " " + verifyProduct.get(i));
        }
    }

    private List<Product> getProductDetails() {
        List<Product> list = new ArrayList<Product>();
        list.add(new Product(1, "Alice Mutton", 0, "Beverages", "CheapestProducts", 4.5000, 10, 23));
        list.add(new Product(2, "Chef Anton", 1, "Condiments", "CheapestProducts", 7.4500, 4, 12));
        list.add(new Product(3, "Gorgonzola", 0, "Produce", "CheapestProducts", 25, 3, 10));
        list.add(new Product(5, "Outback Lager", 1, "Beverages", "CheapestProducts", 3.3, 5, 12));
        list.add(new Product(4, "Perth Pasties", 0, "Seafood", "CheapestProducts", 38, 5, 5));

        return list;
    }

    public void caseInsensitiveOrder() throws Exception {
        List<String> list = DatabaseUtil.getMixedWordsArray("select words from array1 order by upper(words)",
                nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        String[] expwords = { "AbAcUs", "aPPLE", "BlUeBeRrY", "bRaNcH", "cHeRry", "ClOvEr" };
        String[] actwords = list.toArray(new String[list.size()]);
        Assert.assertEquals(actwords, expwords, "Case-insensitive sort failed");
    }

    public void orderByDesc() throws Exception {
        List<Double> list = DatabaseUtil.getDoubArray("select doub  from array1 order by doub desc", nuodbConnection,
                pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        Double[] expdoubles = { 4.1, 3.3, 2.9, 2.3, 1.9, 1.7 };
        Double[] actdoubles = list.toArray(new Double[list.size()]);
        Assert.assertEquals(expdoubles, actdoubles, "Sorting doubles from highest to lowest failed");
    }

    public void stockOrderByDesc() throws Exception {
        List<Product> products = DatabaseUtil.getProductList("select * from products order by unitinstock desc",
                nuodbConnection, pstmt);
        Assert.assertTrue(products.size() >= 1, "The product list is empty");
        List<Product> verifyProduct = getStockOrderByDesc();
        Assert.assertEquals(products.size(), verifyProduct.size(), "Data Mismatch");
        for (int i = 0; i < products.size(); i++) {
            Assert.assertEquals(products.get(i), verifyProduct.get(i),
                    "Object data not matched : Product(" + i + ") " + products.get(i) + " " + verifyProduct.get(i));
        }
    }

    private List<Product> getStockOrderByDesc() {
        List<Product> list = new ArrayList<Product>();
        list.add(new Product(1, "Alice Mutton", 0, "Beverages", "CheapestProducts", 4.5000, 10, 23));
        list.add(new Product(2, "Chef Anton", 1, "Condiments", "CheapestProducts", 7.4500, 4, 12));
        list.add(new Product(5, "Outback Lager", 1, "Beverages", "CheapestProducts", 3.3, 5, 12));
        list.add(new Product(3, "Gorgonzola", 0, "Produce", "CheapestProducts", 25, 3, 10));
        list.add(new Product(4, "Perth Pasties", 0, "Seafood", "CheapestProducts", 38, 5, 5));
        return list;
    }

    public void caseInsensitiveByDesc() throws Exception {
        List<String> list = DatabaseUtil.getMixedWordsArray("select words from array1 order by upper(words) desc",
                nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        String[] expwords = { "ClOvEr", "cHeRry", "bRaNcH", "BlUeBeRrY", "aPPLE", "AbAcUs" };
        String[] actwords = list.toArray(new String[list.size()]);
        Assert.assertEquals(actwords, expwords, "Case-insensitive descending sort failed");
    }

    public void sortByLengthAndAsc() throws Exception {
        List<String> list = DatabaseUtil.getWordsNumArray(
                "select digits from array order by char_length(digits),digits", nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        String[] actwords = list.toArray(new String[list.size()]);
        String[] expwords = { "one", "six", "two", "five", "four", "nine", "zero", "eight", "seven", "three" };
        Assert.assertEquals(actwords, expwords);
    }

    public void sortByLengthAndCaseInsensitive() throws Exception {
        List<String> list = DatabaseUtil.getMixedWordsArray(
                "select words from array1 order by char_length(words),upper(words)", nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        String[] expwords = { "aPPLE", "AbAcUs", "bRaNcH", "cHeRry", "ClOvEr", "BlUeBeRrY" };
        String[] actwords = list.toArray(new String[list.size()]);
        Assert.assertEquals(actwords, expwords);
    }

    public void sortByCategoryAndPriceDesc() throws Exception {
        List<Product> products = DatabaseUtil.getProductList("select * from products order by category,unitprice desc",
                nuodbConnection, pstmt);
        Assert.assertTrue(products.size() >= 1, "The product list is empty");
        List<Product> verifyProduct = getSortByCategoryAndPrice();
        Assert.assertEquals(products.size(), verifyProduct.size(), "Data Mismatch");
        for (int i = 0; i < products.size(); i++) {
            Assert.assertEquals(products.get(i), verifyProduct.get(i),
                    "Object data not matched : Product(" + i + ") " + products.get(i) + " " + verifyProduct.get(i));
        }
    }

    private List<Product> getSortByCategoryAndPrice() {
        List<Product> list = new ArrayList<Product>();
        list.add(new Product(1, "Alice Mutton", 0, "Beverages", "CheapestProducts", 4.5000, 10, 23));
        list.add(new Product(5, "Outback Lager", 1, "Beverages", "CheapestProducts", 3.3, 5, 12));
        list.add(new Product(2, "Chef Anton", 1, "Condiments", "CheapestProducts", 7.4500, 4, 12));
        list.add(new Product(3, "Gorgonzola", 0, "Produce", "CheapestProducts", 25, 3, 10));
        list.add(new Product(4, "Perth Pasties", 0, "Seafood", "CheapestProducts", 38, 5, 5));
        return list;
    }

    public void sortByLengthAndCaseInsensitiveDesc() throws Exception {
        List<String> list = DatabaseUtil.getMixedWordsArray(
                "select words from array1 order by char_length(words),upper(words) desc", nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        String[] expwords = { "aPPLE", "ClOvEr", "cHeRry", "bRaNcH", "AbAcUs", "BlUeBeRrY" };
        String[] actwords = list.toArray(new String[list.size()]);
        Assert.assertEquals(actwords, expwords);
    }

}
