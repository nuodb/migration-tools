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
import com.nuodb.migrator.integration.nuodb.linq.util.Product;

@Test(groups = { "nuodblinqtest" }, dependsOnGroups = { "linqdataloadperformed" })
public class ProjectionTest extends MigrationTestBase {
    PreparedStatement pstmt = null;

    public void numbersPlusOne() throws Exception {
        List<Integer> list = DatabaseUtil.getNumArray("select 1+num as num from array", nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        Integer[] arr1 = list.toArray(new Integer[list.size()]);
        Integer arr2[] = { 6, 5, 2, 4, 10, 9, 7, 8, 3, 1 };
        Assert.assertEquals(arr1, arr2);
    }

    public void namesOfProducts() throws Exception {
        List<Product> products = DatabaseUtil.getProductList("select * from products", nuodbConnection, pstmt);
        Assert.assertTrue(products.size() >= 1, "The product list is empty");
        List<Product> verifyProduct = getProductDetails();
        Collections.sort(products, Product.getSortBasedOnProductID());
        Collections.sort(verifyProduct, Product.getSortBasedOnProductID());
        Assert.assertEquals(products.size(), verifyProduct.size());
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
        list.add(new Product(4, "Perth Pasties", 0, "Seafood", "CheapestProducts", 38, 5, 5));
        list.add(new Product(5, "Outback Lager", 1, "Beverages", "CheapestProducts", 3.3, 5, 12));
        return list;
    }

    public void upperCaseAndLowerCase() throws Exception {
        List<String> list1 = DatabaseUtil.getWordsNumArray("select lower(digits) from array", nuodbConnection, pstmt);
        List<String> list2 = DatabaseUtil.getWordsNumArray("select upper(digits) from array", nuodbConnection, pstmt);
        Assert.assertTrue(list1.size() >= 1, "The list1 is empty");
        Assert.assertTrue(list2.size() >= 1, "The list2 is empty");
        String arr1[] = list1.toArray(new String[list1.size()]);
        String arr2[] = list2.toArray(new String[list2.size()]);

        String arrSmall[] = { "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine" };
        String arrCaps[] = { "ZERO", "ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVEN", "EIGHT", "NINE" };
        Assert.assertEquals(arr1, arrSmall, "Sequence of the uppercase and lowercase are not matching");
        Assert.assertEquals(arr2, arrCaps, "Sequence of the uppercase and lowercase are not matching");
    }

    public void productDetails() throws Exception {
        List<Product> products = DatabaseUtil.getProductList("select * from products", nuodbConnection, pstmt);
        Assert.assertTrue(products.size() >= 1, "The product list is empty");
        List<Product> verifyProduct = getProductDetails();
        Collections.sort(products, Product.getSortBasedOnProductID());
        Collections.sort(verifyProduct, Product.getSortBasedOnProductID());
        Assert.assertEquals(products.size(), verifyProduct.size(), "Data Mismatch");
        for (int i = 0; i < products.size(); i++) {
            Assert.assertEquals(products.get(i), verifyProduct.get(i),
                    "Object data not matched : Product(" + i + ") " + products.get(i) + " " + verifyProduct.get(i));
        }
    }

    public void lessThanTheNumber() throws Exception {
        List<Integer> list1 = DatabaseUtil.getProjectionCompound(
                "select numa from array2 where numa < ( select max(numb) from array3)", nuodbConnection, pstmt);
        Assert.assertTrue(list1.size() >= 1, "The list is empty");
        Integer exparr[] = { 0, 2, 4, 5, 6 };
        Integer[] actarr = list1.toArray(new Integer[list1.size()]);
        Assert.assertEqualsNoOrder(exparr, actarr);
    }

    public void orderTotalLessThan() throws Exception {
        List<Customer> customer = DatabaseUtil.getCustomerList(
                "select * from customers c join orders o on c.customerid=o.customerid where o.total <500",
                nuodbConnection, pstmt);
        Assert.assertTrue(customer.size() >= 1, "The customer list is empty");
        List<Customer> verifyCustList = getOrderTotalLessThan();
        Assert.assertEquals(customer.size(), verifyCustList.size(), "Data Mismatch");
        Collections.sort(customer, Customer.getSortBasedOnCustomerID());
        Collections.sort(verifyCustList, Customer.getSortBasedOnCustomerID());
        for (int i = 0; i < customer.size(); i++) {
            Assert.assertEquals(customer.get(i), verifyCustList.get(i),
                    "Object data not matched : Product(" + i + ") " + customer.get(i) + " " + verifyCustList.get(i));
        }
    }

    private List<Customer> getOrderTotalLessThan() {
        List<Customer> listCust = new ArrayList<Customer>();
        List<Order> listOrd4 = new ArrayList<Order>();
        List<Order> listOrd5 = new ArrayList<Order>();
        Customer c1 = new Customer("Berglunds snabb", 2, "c104", "Canada", listOrd4);
        listOrd4.add(new Order("O10692", "1989-03-15", 320.00, "c104"));
        listCust.add(c1);
        c1 = new Customer("Du monde", 11, "c105", "Washington", listOrd5);
        listOrd5.add(new Order("O10694", "1989-12-01", 88.80, "c105"));
        listCust.add(c1);
        return listCust;
    }

    public void orderDate() throws Exception {
        List<Customer> customer = DatabaseUtil.getCustomerList(
                "select * from customers c join orders o on c.customerid=o.customerid where o.orderdate > "
                        + "'1989-05-29'",
                nuodbConnection, pstmt);
        Assert.assertTrue(customer.size() >= 1, "The customer list is empty");
        List<Customer> verifyCustList = getOrderDate();
        Assert.assertEquals(customer.size(), verifyCustList.size(), "Data Mismatch");
        Collections.sort(customer, Customer.getSortBasedOnCustomerID());
        Collections.sort(verifyCustList, Customer.getSortBasedOnCustomerID());
        for (int i = 0; i < customer.size(); i++) {
            Assert.assertEquals(customer.get(i), verifyCustList.get(i),
                    "Object data not matched : Product(" + i + ") " + customer.get(i) + " " + verifyCustList.get(i));
        }
    }

    private List<Customer> getOrderDate() {
        List<Customer> listCust = new ArrayList<Customer>();

        List<Order> listOrd1 = new ArrayList<Order>();
        List<Order> listOrd4 = new ArrayList<Order>();
        List<Order> listOrd5 = new ArrayList<Order>();
        Customer c1 = new Customer("Alfreds Futterkiste", 8, "c101", "Washington", listOrd1);
        listOrd1.add(new Order("O10690", "2012-09-29", 814.50, "c101"));
        listCust.add(c1);
        c1 = new Customer("Berglunds snabb", 2, "c104", "Canada", listOrd4);
        listOrd4.add(new Order("O10693", "1989-09-16", 2082.00, "c104"));
        listCust.add(c1);
        c1 = new Customer("Du monde", 11, "c105", "Washington", listOrd5);
        listOrd5.add(new Order("O10694", "1989-12-01", 88.80, "c105"));
        listCust.add(c1);
        return listCust;
    }

    public void orderTotalGreaterThan() throws Exception {
        List<Customer> customer = DatabaseUtil.getCustomerList(
                "select * from customers c join orders o on c.customerid=o.customerid where o.total >300",
                nuodbConnection, pstmt);
        Assert.assertTrue(customer.size() >= 1, "The customer list is empty");
        List<Customer> verifyCustList = getOrderTotalGreaterThan();
        Assert.assertEquals(customer.size(), verifyCustList.size(), "Data Mismatch");
        Collections.sort(customer, Customer.getSortBasedOnCustomerID());
        Collections.sort(verifyCustList, Customer.getSortBasedOnCustomerID());
        for (int i = 0; i < customer.size(); i++) {

            Assert.assertEquals(customer.get(i), verifyCustList.get(i),
                    "Object data not matched : Product(" + i + ") " + customer.get(i) + " " + verifyCustList.get(i));
        }
    }

    private List<Customer> getOrderTotalGreaterThan() {
        List<Customer> listCust = new ArrayList<Customer>();
        List<Order> listOrd1 = new ArrayList<Order>();
        List<Order> listOrd2 = new ArrayList<Order>();
        Customer c1 = new Customer("Alfreds Futterkiste", 8, "c101", "Washington", listOrd1);
        listOrd1.add(new Order("O10690", "2012-09-29", 814.50, "c101"));
        listOrd1.add(new Order("O10691", "1989-02-16", 814.50, "c101"));
        listCust.add(c1);
        c1 = new Customer("Berglunds snabb", 2, "c104", "Canada", listOrd2);
        listOrd2.add(new Order("O10692", "1989-03-15", 320.00, "c104"));
        listOrd2.add(new Order("O10693", "1989-09-16", 2082.00, "c104"));
        listCust.add(c1);
        return listCust;
    }

    public void customerFromWashingtonAndOrderDate() throws Exception {
        List<Customer> customer = DatabaseUtil
                .getCustomerList("select c.companyname,c.month,c.customerid,c.region,o.orderid,o.orderdate,o.total,"
                        + "o.customerid from customers c join orders o on c.customerid=o.customerid where o"
                        + ".orderdate > '1990-05-29' and region like 'Wa%'", nuodbConnection, pstmt);
        Assert.assertTrue(customer.size() >= 1, "The customer list is empty");
        List<Customer> verifyCustList = getCustomerFromWashingtonAndOrderDate();
        Assert.assertEquals(customer.size(), verifyCustList.size(), "Data Mismatch");
        Collections.sort(customer, Customer.getSortBasedOnCustomerID());
        Collections.sort(verifyCustList, Customer.getSortBasedOnCustomerID());
        for (int i = 0; i < customer.size(); i++) {

            Assert.assertEquals(customer.get(i), verifyCustList.get(i),
                    "Object data not matched : Product(" + i + ") " + customer.get(i) + " " + verifyCustList.get(i));
        }
    }

    private List<Customer> getCustomerFromWashingtonAndOrderDate() {
        List<Customer> listCust = new ArrayList<Customer>();
        List<Order> listOrd1 = new ArrayList<Order>();
        Customer c1 = new Customer("Alfreds Futterkiste", 8, "c101", "Washington", listOrd1);
        listOrd1.add(new Order("O10690", "2012-09-29", 814.50, "c101"));
        listCust.add(c1);
        return listCust;
    }

    public void customerOrderDetails() throws Exception {
        List<Customer> customer = DatabaseUtil.getCustomerList(
                "select c.companyname,c.month,c.customerid,c.region,o.orderid,o.orderdate,o.total,o.customerid from customers c join orders o on c.customerid=o.customerid",
                nuodbConnection, pstmt);
        Assert.assertTrue(customer.size() >= 1, "The customer list is empty");
        List<Customer> verifyCustList = getCustomerDetails();
        Assert.assertEquals(customer.size(), verifyCustList.size(), "Data Mismatch");
        Collections.sort(customer, Customer.getSortBasedOnCustomerID());
        Collections.sort(verifyCustList, Customer.getSortBasedOnCustomerID());
        for (int i = 0; i < customer.size(); i++) {
            Assert.assertEquals(customer.get(i), verifyCustList.get(i),
                    "Object data not matched : Product(" + i + ") " + customer.get(i) + " " + verifyCustList.get(i));
        }
    }

    private List<Customer> getCustomerDetails() {
        List<Customer> listCust = new ArrayList<Customer>();
        List<Order> listOrd1 = new ArrayList<Order>();
        List<Order> listOrd4 = new ArrayList<Order>();
        List<Order> listOrd5 = new ArrayList<Order>();
        Customer c1 = new Customer("Alfreds Futterkiste", 8, "c101", "Washington", listOrd1);
        listOrd1.add(new Order("O10690", "2012-09-29", 814.50, "c101"));
        listOrd1.add(new Order("O10691", "1989-02-16", 814.50, "c101"));
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
