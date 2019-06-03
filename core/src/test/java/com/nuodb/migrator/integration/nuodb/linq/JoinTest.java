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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.nuodb.migrator.integration.MigrationTestBase;
import com.nuodb.migrator.integration.nuodb.linq.util.DatabaseUtil;
import com.nuodb.migrator.integration.nuodb.linq.util.Product;

@Test(groups = { "nuodblinqtest" }, dependsOnGroups = { "linqdataloadperformed" })
public class JoinTest extends MigrationTestBase {
    PreparedStatement pstmt = null;

    public void crossJoin() throws Exception {
        List<String> list = DatabaseUtil.getJoinArray(nuodbConnection, pstmt);
        String[] categories = list.toArray(new String[list.size()]);
        List<Product> products = DatabaseUtil.getProductList(
                "select * from products cross join array5 where cate_name=category", nuodbConnection, pstmt);
        Assert.assertTrue(products.size() >= 1, "The product list is empty");
        HashMap<String, String> actMap = new HashMap<String, String>();
        HashMap<String, String> expMap = new HashMap<String, String>();
        List<String> strObj = new ArrayList<String>();
        Assert.assertNotNull(list);
        Assert.assertNotNull(products);
        for (String str : categories) {
            strObj.add(str);
        }
        for (Product p : products) {
            if (strObj.contains(p.getCategory())) {
                actMap.put(p.getPname(), p.getCategory());
            }
        }
        expMap.put("Alice Mutton", "Beverages");
        expMap.put("Outback Lager", "Beverages");
        expMap.put("Chef Anton", "Condiments");
        Assert.assertEquals(actMap, expMap);
    }

    public void groupJoin() throws Exception {
        List<String> list = DatabaseUtil.getJoinArray(nuodbConnection, pstmt);
        String[] categories = list.toArray(new String[list.size()]);
        List<Product> products = DatabaseUtil
                .getProductList("select * from products join array5 where cate_name=category", nuodbConnection, pstmt);
        Assert.assertTrue(products.size() >= 1, "The product list is empty");
        HashMap<String, ArrayList<String>> actmap = new HashMap<String, ArrayList<String>>();
        for (Product p : products) {
            if (DatabaseUtil.contains(p.getCategory(), categories)) {
                if (!actmap.containsKey(p.getCategory())) {
                    ArrayList<String> list1 = new ArrayList<String>();
                    list1.add(p.getPname());
                    Collections.sort(list1);
                    actmap.put(p.getCategory(), list1);
                } else {
                    ArrayList<String> list1 = (ArrayList<String>) actmap.get(p.getCategory());
                    list1.add(p.getPname());
                    Collections.sort(list1);
                    actmap.put(p.getCategory(), list1);
                }
            }
        }
        ArrayList<String> catelist1 = new ArrayList<String>();
        ArrayList<String> catelist2 = new ArrayList<String>();
        HashMap<String, ArrayList<String>> expmap = new HashMap<String, ArrayList<String>>();
        catelist1.add("Chef Anton");
        Collections.sort(catelist1);
        expmap.put("Condiments", catelist1);
        catelist2.add("Alice Mutton");
        catelist2.add("Outback Lager");
        Collections.sort(catelist2);
        expmap.put("Beverages", catelist2);
        Assert.assertEquals(actmap, expmap);
    }

    public void crossAndGroupJoin() throws Exception {
        List<String> list = DatabaseUtil.getJoinArray(nuodbConnection, pstmt);
        String[] categories = list.toArray(new String[list.size()]);
        List<Product> products = DatabaseUtil
                .getProductList("select * from products  join array5 where cate_name=category", nuodbConnection, pstmt);
        List<String> strObj = new ArrayList<String>();
        Assert.assertTrue(products.size() >= 1, "The product list is empty");
        HashMap<String, String> actMap = new HashMap<String, String>();
        HashMap<String, String> expMap = new HashMap<String, String>();
        for (String str : categories) {
            strObj.add(str);
        }
        for (Product p : products) {
            if (strObj.contains(p.getCategory())) {
                actMap.put(p.getPname(), p.getCategory());
            }
        }
        expMap.put("Alice Mutton", "Beverages");
        expMap.put("Outback Lager", "Beverages");
        expMap.put("Chef Anton", "Condiments");
        Assert.assertEquals(actMap, expMap);
    }

    public void leftOuterJoin() throws Exception {
        TreeMap<String, String> map = DatabaseUtil.getJoinProductList(nuodbConnection, pstmt);
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
        HashMap<String, String> actMap = new HashMap<String, String>();
        HashMap<String, String> expMap = new HashMap<String, String>();
        Assert.assertTrue(map.size() >= 1, "The map list is empty");
        while (it.hasNext()) {
            Map.Entry<String, String> pairs = (Map.Entry<String, String>) it.next();
            String key = (String) pairs.getKey();
            String value = (String) pairs.getValue();
            actMap.put(key, value);
        }
        expMap.put("Alice Mutton", "Beverages");
        expMap.put("Outback Lager", "Beverages");
        expMap.put("Chef Anton", "Condiments");
        expMap.put("", "Vegetables");
        Assert.assertEquals(actMap, expMap);
    }
}
