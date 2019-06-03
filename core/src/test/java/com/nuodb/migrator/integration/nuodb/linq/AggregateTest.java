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
import java.util.HashMap;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.nuodb.migrator.integration.MigrationTestBase;
import com.nuodb.migrator.integration.nuodb.linq.util.DatabaseUtil;

@Test(groups = { "nuodblinqtest" }, dependsOnGroups = { "linqdataloadperformed" })
public class AggregateTest extends MigrationTestBase {
    PreparedStatement pstmt = null;

    public void uniqueFactor() throws Exception {
        List<Integer> list = DatabaseUtil.getRepeatNumArray(
                "select distinct mod(repeatnum,300) as repeatnum from array3", nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        Assert.assertEquals(list.size(), 3, "Unique factors count mismatching");
    }

    public void oddCount() throws Exception {
        int oddNum = DatabaseUtil.getAggreNum("SELECT count(num) FROM array WHERE cast (num as int) % 2 = 0",
                nuodbConnection, pstmt);
        Assert.assertEquals(oddNum, 5, "Odd number count mismatching");
    }

    public void orderCount() throws Exception {
        HashMap<String, Integer> map = DatabaseUtil.getAgreOrderList(
                "select count(*), customerid from orders group by customerid", nuodbConnection, pstmt);
        Assert.assertTrue(map.size() >= 1, "The map is empty");
        HashMap<String, Integer> expMap = new HashMap<String, Integer>();
        Assert.assertNotNull(map, "The orderList is empty");
        expMap.put("c101", 2);
        expMap.put("c104", 2);
        expMap.put("c105", 1);
        Assert.assertEquals(map, expMap, "Order count mismatching");
    }

    public void categoryCount() throws Exception {
        HashMap<String, Integer> map = DatabaseUtil.getAgreProductList(
                "select count(pid),category from products group by category", nuodbConnection, pstmt);
        Assert.assertTrue(map.size() >= 1, "The map is empty");
        HashMap<String, Integer> expMap = new HashMap<String, Integer>();
        expMap.put("Beverages", 2);
        expMap.put("Condiments", 1);
        expMap.put("Produce", 1);
        expMap.put("Seafood", 1);
        Assert.assertEquals(map, expMap, "Category count mismatching");
    }

    public void totalOfNumbers() throws Exception {
        int sum = DatabaseUtil.getAggreNum("select sum(num) from array", nuodbConnection, pstmt);
        Assert.assertEquals(sum, 45, "Sum of the numbers are mismatching");
    }

    public void totalOfCharacters() throws Exception {
        List<Integer> list = DatabaseUtil.getIntReturn("select sum(char_length(digits)) as digits from array",
                nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        List<Integer> list1 = new ArrayList<Integer>();
        list1.add(40);
        Assert.assertEquals(list, list1, "Total number of characters are mismatching");
    }

    public void stockCount() throws Exception {
        HashMap<String, Integer> map = DatabaseUtil.getAgreProductList(
                "select sum(unitinstock),category from products group by category", nuodbConnection, pstmt);
        Assert.assertTrue(map.size() >= 1, "The map is empty");
        Assert.assertNotNull(map, "The product List is empty");
        HashMap<String, Integer> expMap = new HashMap<String, Integer>();
        expMap.put("Beverages", 35);
        expMap.put("Condiments", 12);
        expMap.put("Produce", 10);
        expMap.put("Seafood", 5);
        Assert.assertEquals(map, expMap);
    }

    public void lowestNumber() throws Exception {
        int minNum = DatabaseUtil.getAggreNum("select min(num) from array", nuodbConnection, pstmt);
        Assert.assertEquals(minNum, 0, "Lowest number mismatching");

    }

    public void shortestWordLength() throws Exception {
        List<Integer> actlist = DatabaseUtil.getIntReturn("select min(char_length(digits)) as digits from array",
                nuodbConnection, pstmt);
        Assert.assertTrue(actlist.size() >= 1, "The actual list is empty");
        List<Integer> explist = new ArrayList<Integer>();
        explist.add(3);
        Assert.assertEquals(actlist, explist, "Length of the shortest word mismatching");
    }

    public void cheapestPrice() throws Exception {
        HashMap<String, Double> map = DatabaseUtil.getAgreDoubProductList(
                "select min(unitprice),category from products group by category", nuodbConnection, pstmt);
        Assert.assertTrue(map.size() >= 1, "The map is empty");
        HashMap<String, Double> expMap = new HashMap<String, Double>();
        expMap.put("Beverages", 3.3);
        expMap.put("Condiments", 7.45);
        expMap.put("Produce", 25D);
        expMap.put("Seafood", 38D);
        Assert.assertEquals(map, expMap);
    }

    /*
     * public void cheapestPriceDetails() throws Exception { List<Product>
     * products = UtilBasicTest .getProductList( "SELECT A.category,
     * a.unitprice, A.pid FROM products AS A INNER JOIN (SELECT C.category as
     * minCat, MIN(C.unitprice) AS UP2 FROM products AS C GROUP BY C.category)
     * AS B ON A.category = B.minCat" ,nuodbConnection,rs,pstmt);
     * Assert.assertNotNull(products); for (Product p : products) {
     * Assert.assertNotNull(p); } }
     * 
     * public void cheapestPriceDetails() throws Exception { List<Product>
     * products = UtilBasicTest .getProductList( "SELECT A.category,
     * a.unitprice, A.pid FROM products AS A INNER JOIN (SELECT C.category as
     * minCat, MIN(C.unitprice) AS UP2 FROM products AS C GROUP BY C.category)
     * AS B ON A.category = B.minCat and a.unitprice=b .up2 GROUP by
     * a.category,a.pid" ,nuodbConnection,rs,pstmt);
     * Assert.assertNotNull(products); for (Product p : products) {
     * Assert.assertNotNull(p); } }
     */

    public void highestNumber() throws Exception {
        int maxNum = DatabaseUtil.getAggreNum("select max(num) from array", nuodbConnection, pstmt);
        Assert.assertEquals(maxNum, 9, "Highest number mismatching");
    }

    public void longestWordLength() throws Exception {
        List<Integer> actlist = DatabaseUtil.getIntReturn("select max(char_length(digits)) as digits from array",
                nuodbConnection, pstmt);
        Assert.assertTrue(actlist.size() >= 1, "The actual list is empty");
        List<Integer> explist = new ArrayList<Integer>();
        explist.add(5);
        Assert.assertEquals(actlist, explist, "Length of the longest word mismatching");
    }

    public void expensivePrice() throws Exception {
        HashMap<String, Double> map = DatabaseUtil.getAgreDoubProductList(
                "select max(unitprice),category from products group by category", nuodbConnection, pstmt);
        Assert.assertTrue(map.size() >= 1, "The map is empty");
        HashMap<String, Double> expMap = new HashMap<String, Double>();
        expMap.put("Beverages", 4.5);
        expMap.put("Condiments", 7.45);
        expMap.put("Produce", 25D);
        expMap.put("Seafood", 38D);
        Assert.assertEquals(map, expMap);
    }

    /*
     * public void expensivePriceDetails() throws Exception { List<Product>
     * products = UtilBasicTest .getProductList( "SELECT A.category,
     * a.unitprice, A.pid FROM products AS A INNER JOIN (SELECT C.category as
     * maxCat, MAX(C.unitprice) AS UP2 FROM products AS C GROUP BY C.category)
     * AS B ON A.category = B.maxCat" ,nuodbConnection,rs,pstmt);
     * Assert.assertNotNull(products); for (Product p : products) {
     * Assert.assertNotNull(p); } }
     * 
     * public void expensivePriceDetails() throws Exception { List<Product>
     * products = UtilBasicTest .getProductList( "SELECT A.category,
     * a.unitprice, A.pid FROM products AS A INNER JOIN (SELECT C.category as
     * maxCat, MAX(C.unitprice) AS UP2 FROM products AS C GROUP BY C.category)
     * AS B ON A.category = B.maxCat and a.unitprice=b .up2 GROUP by
     * a.category,a.pid" ,nuodbConnection,rs,pstmt);
     * Assert.assertNotNull(products); for (Product p : products) {
     * Assert.assertNotNull(p); } }
     */

    public void averageOfNumbers() throws Exception {
        int averageNum = DatabaseUtil.getAggreNum("select avg(num) from array", nuodbConnection, pstmt);
        Assert.assertEquals(averageNum, 4, "Average result mismatching");
    }

    public void averageWordLenth() throws Exception {
        List<Integer> list = DatabaseUtil.getIntReturn("select avg(char_length(digits)) as digits from array",
                nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        List<Integer> list1 = new ArrayList<Integer>();
        list1.add(4);
        Assert.assertEquals(list, list1);
    }

    public void averagePrice() throws Exception {
        HashMap<String, Double> map = DatabaseUtil.getAgreDoubProductList(
                "select avg(unitprice),category from products group by category", nuodbConnection, pstmt);
        Assert.assertTrue(map.size() >= 1, "The map is empty");
        HashMap<String, Double> expMap = new HashMap<String, Double>();
        expMap.put("Beverages", 3.9);
        expMap.put("Condiments", 7.45);
        expMap.put("Produce", 25D);
        expMap.put("Seafood", 38D);
        Assert.assertEquals(map, expMap);
    }

}
