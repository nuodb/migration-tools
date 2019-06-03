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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.nuodb.migrator.integration.MigrationTestBase;
import com.nuodb.migrator.integration.nuodb.linq.util.DatabaseUtil;

@Test(groups = { "nuodblinqtest" }, dependsOnGroups = { "linqdataloadperformed" })
public class MiscellaneousTest extends MigrationTestBase {
    PreparedStatement pstmt = null;

    public void concatNumbers() throws Exception {
        List<Integer> list = DatabaseUtil.getUnion("select numa from array2 union select numb from array3",
                nuodbConnection, pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        Integer[] actarr = list.toArray(new Integer[list.size()]);
        Integer exparr[] = { 0, 2, 4, 5, 6, 8, 9, 1, 3, 5, 7, 8 };
        Assert.assertEqualsNoOrder(actarr, exparr);
    }

    public void concatProductAndCustomerDetails() throws Exception {
        List<String> actList = DatabaseUtil.getUnionNames(
                "select pname from products union select companyname from customers", nuodbConnection, pstmt);
        Assert.assertTrue(actList.size() >= 1, "The actual list is empty");
        List<String> expList = new ArrayList<String>();
        expList.add("Alice Mutton");
        expList.add("Chef Anton");
        expList.add("Gorgonzola");
        expList.add("Perth Pasties");
        expList.add("Outback Lager");
        expList.add("Alfreds Futterkiste");
        expList.add("Ana Trujillo");
        expList.add("Around the Horn");
        expList.add("Berglunds snabb");
        expList.add("Du monde");
        Collections.sort(expList);
        Collections.sort(actList);
        Assert.assertEquals(actList, expList, "Names of all customers and products are mismatching");
    }

    public void findMatching() throws Exception {
        List<Integer> list = DatabaseUtil.getNumBArray("select numb from array3 where (numb = numb)", nuodbConnection,
                pstmt);
        Assert.assertTrue(list.size() >= 1, "The list is empty");
        Integer[] actarr = list.toArray(new Integer[list.size()]);
        Integer[] exparr = { 1, 3, 5, 7, 8 };
        Assert.assertEqualsNoOrder(actarr, exparr);
    }

    public void findMismatching() throws Exception {
        List<String> list1 = DatabaseUtil.getWordsNumArray("select * from array", nuodbConnection, pstmt);
        Assert.assertTrue(list1.size() >= 1, "The list is empty");
        String[] wordsA = list1.toArray(new String[list1.size()]);
        String[] expwords = { "aPPLE", "AbAcUs", "bRaNcH", "BlUeBeRrY", "ClOvEr" };
        Assert.assertNotNull(wordsA, "Empty array");
        boolean b = Arrays.equals(wordsA, expwords);
        Assert.assertFalse(b, "Two array is matched");
    }
}
