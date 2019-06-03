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
import com.nuodb.migrator.integration.nuodb.linq.util.Customer;
import com.nuodb.migrator.integration.nuodb.linq.util.DatabaseUtil;
import com.nuodb.migrator.integration.nuodb.linq.util.Order;

@Test(groups = { "nuodblinqtest" }, dependsOnGroups = { "linqdataloadperformed" })
public class PartitioningTest extends MigrationTestBase {
    PreparedStatement pstmt = null;

    public void firstFewElements() throws Exception {
        List<Integer> list = DatabaseUtil.getNumArray("select num from array limit 3", nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        Integer[] numbers = list.toArray(new Integer[list.size()]);
        Integer exparr[] = { 5, 4, 1 };
        Assert.assertEqualsNoOrder(numbers, exparr, "Not first 3 elements of the array");
    }

    public void firstCustomerDetails() throws Exception {
        List<Customer> customer = DatabaseUtil
                .getCustomerList("select c.companyname,c.month,c.customerid,c.region,o.orderid,o.orderdate,o.total,"
                        + "o.customerid from customers c join orders o on c.customerid=o.customerid where "
                        + "region like 'Wa%' limit 2", nuodbConnection, pstmt);
        Assert.assertTrue(customer.size() >= 1, "The customer list is empty");
        List<Customer> verifyCustList = getFirstCustomerDetails();
        Assert.assertEquals(customer.size(), verifyCustList.size(), "Data Mismatch");
        Collections.sort(customer, Customer.getSortBasedOnCustomerID());
        Collections.sort(verifyCustList, Customer.getSortBasedOnCustomerID());
        for (int i = 0; i < customer.size(); i++) {
            Assert.assertEquals(customer.get(i), verifyCustList.get(i),
                    "Object data not matched : Product(" + i + ") " + customer.get(i) + " " + verifyCustList.get(i));
        }
    }

    private List<Customer> getFirstCustomerDetails() {
        List<Customer> listCust = new ArrayList<Customer>();
        List<Order> listOrd1 = new ArrayList<Order>();
        Customer c1 = new Customer("Alfreds Futterkiste", 8, "c101", "Washington", listOrd1);
        listOrd1.add(new Order("O10690", "2012-09-29", 814.50, "c101"));
        listOrd1.add(new Order("O10691", "1989-02-16", 814.50, "c101"));
        listCust.add(c1);
        return listCust;
    }

    public void firstElementsSkipped() throws Exception {
        List<Integer> list = DatabaseUtil.getNumArray("select num from array offset 4", nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        Integer[] actarr = list.toArray(new Integer[list.size()]);
        Integer exparr[] = { 9, 8, 6, 7, 2, 0 };
        Assert.assertEqualsNoOrder(actarr, exparr, "First 4 elements of the array not skipped");
    }

    public void detailsBasedCustomerRegion() throws Exception {
        List<Customer> customer = DatabaseUtil
                .getCustomerList("select c.companyname,c.month,c.customerid,c.region,o.orderid,o.orderdate,o.total,"
                        + "o.customerid from customers c join orders o on c.customerid=o.customerid where "
                        + "region like 'Wa%'  offset 2", nuodbConnection, pstmt);
        Assert.assertTrue(customer.size() >= 1, "The customer list is empty");
        List<Customer> verifyCustList = getDetailsBasedCustomerRegion();
        Assert.assertEquals(customer.size(), verifyCustList.size(), "Data Mismatch");
        Collections.sort(customer, Customer.getSortBasedOnCustomerID());
        Collections.sort(verifyCustList, Customer.getSortBasedOnCustomerID());
        for (int i = 0; i < customer.size(); i++) {
            Assert.assertEquals(customer.get(i), verifyCustList.get(i),
                    "Object data not matched : Product(" + i + ") " + customer.get(i) + " " + verifyCustList.get(i));
        }
    }

    private List<Customer> getDetailsBasedCustomerRegion() {
        List<Customer> listCust = new ArrayList<Customer>();
        List<Order> listOrd5 = new ArrayList<Order>();
        Customer c1 = new Customer("Du monde", 11, "c105", "Washington", listOrd5);
        listOrd5.add(new Order("O10694", "1989-12-01", 88.80, "c105"));
        listCust.add(c1);
        return listCust;
    }

}
