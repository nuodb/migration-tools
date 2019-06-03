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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;

import org.testng.Assert;

public class DatabaseUtil {

    public static List<String> getWordsNumArray(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            List<String> list = new ArrayList<String>();
            boolean found = false;
            while (rs2.next()) {
                found = true;
                list.add(rs2.getString("digits"));
            }
            Assert.assertTrue(found);
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<Integer> getUnion(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            List<Integer> list = new ArrayList<Integer>();
            boolean found = false;
            while (rs2.next()) {
                found = true;
                list.add(rs2.getInt(1));
            }
            Assert.assertTrue(found);
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<Integer> getProjectionCompound(String sqlStr, Connection nuodbConnection,
            PreparedStatement stmt2) throws Exception {
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            List<Integer> list = new ArrayList<Integer>();
            boolean found = false;
            while (rs2.next()) {
                found = true;
                list.add(rs2.getInt("numa"));
            }
            Assert.assertTrue(found);
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<Integer> getIntReturn(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            List<Integer> list = new ArrayList<Integer>();
            boolean found = false;
            while (rs2.next()) {
                found = true;
                list.add(rs2.getInt("digits"));
            }
            Assert.assertTrue(found);
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<Integer> getNumCArray(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            List<Integer> list = new ArrayList<Integer>();
            boolean found = false;
            while (rs2.next()) {
                found = true;
                list.add(rs2.getInt("numc"));
            }
            Assert.assertTrue(found);
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<Integer> getNumBArray(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            List<Integer> list = new ArrayList<Integer>();
            while (rs2.next()) {
                list.add(rs2.getInt("numb"));
            }
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<Integer> getVector(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            List<Integer> list = new ArrayList<Integer>();
            boolean found = false;
            while (rs2.next()) {
                found = true;
                list.add(rs2.getInt("dot"));
            }
            Assert.assertTrue(found);
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<Integer> getRepeatNumArray(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            List<Integer> list = new ArrayList<Integer>();
            boolean found = false;
            while (rs2.next()) {
                found = true;
                list.add(rs2.getInt("repeatnum"));
            }
            Assert.assertTrue(found);
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<String> getJoinArray(Connection nuodbConnection, PreparedStatement stmt2) throws Exception {
        String sqlStr = "select * from array5";
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            List<String> list = new ArrayList<String>();
            boolean found = false;
            while (rs2.next()) {
                found = true;
                list.add(rs2.getString("cate_name"));
            }
            Assert.assertTrue(found);
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static int getAggreNum(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2) throws Exception {
        ResultSet rs2 = null;
        try {
            int result = 0;
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            boolean found = false;
            while (rs2.next()) {
                found = true;
                result = rs2.getInt(1);
            }
            Assert.assertTrue(found);
            Assert.assertNotNull(rs2);
            return result;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<Double> getDoubArray(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            List<Double> list = new ArrayList<Double>();
            boolean found = false;
            while (rs2.next()) {
                found = true;
                list.add(rs2.getDouble("doub"));
            }
            Assert.assertTrue(found);
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<Integer> getNumArray(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            List<Integer> list = new ArrayList<Integer>();
            boolean found = false;
            while (rs2.next()) {
                found = true;
                list.add(rs2.getInt("num"));
            }
            Assert.assertTrue(found);
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<String> getMixedWordsArray(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            List<String> list = new ArrayList<String>();
            boolean found = false;
            while (rs2.next()) {
                found = true;
                list.add(rs2.getString("words"));
            }
            Assert.assertTrue(found);
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<String> getSimWordsArray(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            List<String> list = new ArrayList<String>();
            boolean found = false;
            while (rs2.next()) {
                found = true;
                list.add(rs2.getString("similarword"));
            }
            Assert.assertTrue(found);
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static HashMap<String, Integer> getProductListGroupBy(String sqlStr, Connection nuodbConnection,
            PreparedStatement stmt2) throws Exception {
        ResultSet rs2 = null;
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            while (rs2.next()) {
                String key = rs2.getString("category");
                Integer value = rs2.getInt("sum");
                map.put(key, value);
            }
            Assert.assertNotNull(rs2);
            return map;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<String> getDistinctProductList(String sqlStr, Connection nuodbConnection,
            PreparedStatement stmt2) throws Exception {
        ResultSet rs2 = null;
        List<String> list = new ArrayList<String>();
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            while (rs2.next()) {
                list.add(rs2.getString("category"));
            }
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<Product> getProductList(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        ResultSet rs2 = null;
        List<Product> list = new ArrayList<Product>();
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            while (rs2.next()) {
                Product p1 = new Product(rs2.getInt("pid"), rs2.getString("pname"), rs2.getInt("stock"),
                        rs2.getString("category"), rs2.getString("cheapestproduct"), rs2.getDouble("unitprice"),
                        rs2.getInt("productcount"), rs2.getInt("unitinstock"));
                list.add(p1);
            }
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<String> getUnionNames(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        ResultSet rs2 = null;
        List<String> list = new ArrayList<String>();
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            while (rs2.next()) {
                list.add(rs2.getString(1));
            }
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<Customer> getCustomerList(Connection nuodbConnection, PreparedStatement stmt2) throws Exception {
        String sqlStr = "select * from customers";
        List<Customer> list = new ArrayList<Customer>();
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            List<Order> list1 = null;
            while (rs2.next()) {
                list1 = getOrderList(rs2.getString("customerid"), nuodbConnection, stmt2);
                Customer c1 = new Customer(rs2.getString("companyname"), rs2.getInt("month"),
                        rs2.getString("customerid"), rs2.getString("region"), list1);
                list.add(c1);
            }
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<Order> getOrderList(String custId, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        String sqlStr = "select * from orders where customerid='" + custId + "'";
        ResultSet rs2 = null;
        List<Order> list = new ArrayList<Order>();
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            while (rs2.next()) {
                Order o1 = new Order(rs2.getString("orderid"), rs2.getString("orderdate"), rs2.getDouble("total"),
                        rs2.getString("customerid"));
                list.add(o1);
            }
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<Customer> getCustomerList(String sqlStr, Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        ResultSet rs2 = null;
        List<Customer> list = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            HashMap<String, Customer> cList = new HashMap<String, Customer>();
            while (rs2.next()) {

                Customer c1 = null;
                String cid = rs2.getString("customerid");
                if (cList.get(cid) != null) {
                    c1 = cList.get(cid);
                    List<Order> l = c1.getList1();
                    l.add(new Order(rs2.getString("orderid"), rs2.getString("orderdate"), rs2.getDouble("total"),
                            rs2.getString("customerid")));

                } else {
                    c1 = new Customer(cid);
                    c1.setCompanyname(rs2.getString("companyname"));
                    c1.setCustomerid(rs2.getString("customerid"));
                    c1.setMonth(rs2.getInt("month"));
                    c1.setRegion(rs2.getString("region"));
                    List<Order> l = c1.getList1();
                    l.add(new Order(rs2.getString("orderid"), rs2.getString("orderdate"), rs2.getDouble("total"),
                            rs2.getString("customerid")));
                    cList.put(cid, c1);
                }
            }
            list = new ArrayList<Customer>(cList.values());
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static List<Customer> getGroupByCustomerList(String sqlStr, Connection nuodbConnection,
            PreparedStatement stmt2) throws Exception {
        ResultSet rs2 = null;
        List<Customer> list = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            LinkedHashMap<String, Customer> cList = new LinkedHashMap<String, Customer>();
            while (rs2.next()) {

                Customer c1 = null;
                String cid = rs2.getString("customerid");
                if (cList.get(cid) != null) {
                    c1 = cList.get(cid);
                    List<Order> l = c1.getList1();
                    l.add(new Order(rs2.getString("orderid"), rs2.getString("orderdate"), rs2.getDouble("total"),
                            rs2.getString("customerid")));

                } else {
                    c1 = new Customer(cid);
                    c1.setCompanyname(rs2.getString("companyname"));
                    c1.setCustomerid(rs2.getString("customerid"));
                    c1.setMonth(rs2.getInt("month"));
                    c1.setRegion(rs2.getString("region"));
                    List<Order> l = c1.getList1();
                    l.add(new Order(rs2.getString("orderid"), rs2.getString("orderdate"), rs2.getDouble("total"),
                            rs2.getString("customerid")));
                    cList.put(cid, c1);
                }
            }
            list = new ArrayList<Customer>(cList.values());
            Assert.assertNotNull(rs2);
            return list;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static TreeMap<String, String> getJoinProductList(Connection nuodbConnection, PreparedStatement stmt2)
            throws Exception {
        String sqlStr = "select p.pname,p.pid,a.cate_name from array5 a left join products p on a.cate_name=p.category";
        TreeMap<String, String> map = new TreeMap<String, String>();
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            while (rs2.next()) {
                String pname = rs2.getString("pname");
                String cate = rs2.getString("cate_name");
                map.put(pname == null ? "" : pname, cate);
            }
            Assert.assertNotNull(rs2);
            return map;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static HashMap<String, Integer> getAgreProductList(String sqlStr, Connection nuodbConnection,
            PreparedStatement stmt2) throws Exception {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            while (rs2.next()) {
                map.put(rs2.getString(2), rs2.getInt(1));
            }
            Assert.assertNotNull(rs2);
            return map;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static HashMap<String, Double> getAgreDoubProductList(String sqlStr, Connection nuodbConnection,
            PreparedStatement stmt2) throws Exception {
        HashMap<String, Double> map = new HashMap<String, Double>();
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            while (rs2.next()) {
                map.put(rs2.getString(2), rs2.getDouble(1));
            }
            Assert.assertNotNull(rs2);
            return map;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static HashMap<String, Integer> getAgreOrderList(String sqlStr, Connection nuodbConnection,
            PreparedStatement stmt2) throws Exception {
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        ResultSet rs2 = null;
        try {
            stmt2 = nuodbConnection.prepareStatement(sqlStr);
            rs2 = stmt2.executeQuery();
            while (rs2.next()) {
                map.put(rs2.getString(2), rs2.getInt(1));
            }
            Assert.assertNotNull(rs2);
            return map;
        } finally {
            closeAll(rs2, stmt2);
        }
    }

    public static boolean contains(String key, String[] categories) {
        for (String str : categories) {
            if (str.equalsIgnoreCase(key)) {
                return true;
            }
        }
        return false;
    }

    protected static void closeAll(ResultSet rs1, Statement stmt1) throws SQLException {
        if (rs1 != null) {
            rs1.close();
        }
        if (stmt1 != null) {
            stmt1.close();
        }
    }
}
