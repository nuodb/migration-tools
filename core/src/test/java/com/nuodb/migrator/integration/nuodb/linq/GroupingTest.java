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
import com.nuodb.migrator.integration.nuodb.linq.util.Customer;
import com.nuodb.migrator.integration.nuodb.linq.util.DatabaseUtil;
import com.nuodb.migrator.integration.nuodb.linq.util.Order;

@Test(groups = { "nuodblinqtest" }, dependsOnGroups = { "linqdataloadperformed" })
public class GroupingTest extends MigrationTestBase {
    PreparedStatement pstmt = null;

    public void groupByWord() throws Exception {
        List<String> actlist = DatabaseUtil.getMixedWordsArray(
                "select words from array1 group by UPPER(substr(words,1,2))", nuodbConnection, pstmt);
        Assert.assertTrue(actlist.size() >= 1, "The list is empty");
        Assert.assertTrue("AbAcUs".equals(actlist.get(0)) || "aPPLE".equals(actlist.get(0)));
        Assert.assertTrue("AbAcUs".equals(actlist.get(1)) || "aPPLE".equals(actlist.get(1)));
        Assert.assertTrue("BlUeBeRrY".equals(actlist.get(2)) || "bRaNcH".equals(actlist.get(2)));
        Assert.assertTrue("BlUeBeRrY".equals(actlist.get(3)) || "bRaNcH".equals(actlist.get(3)));
        Assert.assertTrue("cHeRry".equals(actlist.get(4)) || "ClOvEr".equals(actlist.get(4)));
        Assert.assertTrue("cHeRry".equals(actlist.get(5)) || "ClOvEr".equals(actlist.get(5)));

    }

    public void groupByCategory() throws Exception {
        HashMap<String, Integer> map = DatabaseUtil.getProductListGroupBy(
                "select sum(unitinstock),category from products group by category", nuodbConnection, pstmt);
        Assert.assertTrue(map.size() >= 1, "The map is empty");
        HashMap<String, Integer> expMap = new HashMap<String, Integer>();
        expMap.put("Beverages", 35);
        expMap.put("Condiments", 12);
        expMap.put("Produce", 10);
        expMap.put("Seafood", 5);
        Assert.assertEquals(map, expMap);
    }

    public void groupByCustomerOrderAndDate() throws Exception {
        List<Customer> customer = DatabaseUtil.getGroupByCustomerList(
                "select c.companyname,c.month,c.customerid,c.region,o.orderid,o.orderdate,o.total,"
                        + "o.customerid from customers c join orders o on c.customerid=o.customerid group by o"
                        + ".orderid order by o.orderdate",
                nuodbConnection, pstmt);
        Assert.assertTrue(customer.size() >= 1, "The customer list is empty");
        List<Customer> verifyCustList = getGroupByCustomerOrderAndDate();
        Assert.assertEquals(customer.size(), verifyCustList.size(), "Data Mismatch");
        for (int i = 0; i < customer.size(); i++) {
            Assert.assertEquals(customer.get(i), verifyCustList.get(i),
                    "Object data not matched : Product(" + i + ") " + customer.get(i) + " " + verifyCustList.get(i));
        }
    }

    private List<Customer> getGroupByCustomerOrderAndDate() {
        List<Customer> listCust = new ArrayList<Customer>();

        List<Order> listOrd1 = new ArrayList<Order>();
        List<Order> listOrd4 = new ArrayList<Order>();
        List<Order> listOrd5 = new ArrayList<Order>();
        Customer c1 = new Customer("Alfreds Futterkiste", 8, "c101", "Washington", listOrd1);
        listOrd1.add(new Order("O10691", "1989-02-16", 814.50, "c101"));
        listOrd1.add(new Order("O10690", "2012-09-29", 814.50, "c101"));
        listCust.add(c1);
        c1 = new Customer("Berglunds snabb", 2, "c104", "Canada", listOrd4);
        listOrd4.add(new Order("O10692", "1989-03-15", 320.00, "c104"));
        listOrd4.add(new Order("O10693", "1989-09-16", 2082.00, "c104"));
        listCust.add(c1);
        c1 = new Customer("Du monde", 11, "c105", "Washington", listOrd5);
        listOrd5.add(new Order("O10694", "1989-12-01", 88.80, "c105"));
        listCust.add(c1);
        return listCust;
    }
}
