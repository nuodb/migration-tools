
/**
 * Copyright (c) 2012, NuoDB, Inc.
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
package com.nuodb.migrator.integration.precision;


import java.sql.Connection;
import java.util.ArrayList;

public class PrecisionConstantMySql {

	public static final String NUODB_JDBC_JAR = "nuodbjdbc.jar";

	protected Connection sourceConnection;
	
	public static ArrayList<MySqlDataPrecision1> getMySqlDataPrecision1()
	{
		/*
	  long l1= -9223372036854775807L;
		long l2= -2147483648L;
		long l3= -8388608L;
		*/
		long l1= 0L;
    long l2= 0L;
    long l3= 0L;
    
    
		MySqlDataPrecision1 m1=new MySqlDataPrecision1(66,2687,678246,49,3720368547758L);
		MySqlDataPrecision1 m2=new MySqlDataPrecision1(127,32767,8388607,2147483647,9223372036854775807L);
		MySqlDataPrecision1 m3=new MySqlDataPrecision1(-128,-32768,l3,l2,l1);
		ArrayList<MySqlDataPrecision1> t1List=new ArrayList<MySqlDataPrecision1>();
		t1List.add(m1);
		t1List.add(m2);
		t1List.add(m3);
		return t1List;
	}
	public static ArrayList<MySqlDataPrecision2> getMySqlDataPrecision2()
	{
		/* Original values are changed to avoid float data type issue */ 
		MySqlDataPrecision2 m4=new MySqlDataPrecision2("sample text","sample data",23.0,4.599999904632568,416.7,"true","1234567890");
		MySqlDataPrecision2 m5=new MySqlDataPrecision2("sample text length20","total word lenght 20",1.2345678E7,9.8765432E7,34567891.17,"false","12345678900123456789");
		MySqlDataPrecision2 m6=new MySqlDataPrecision2("","sample data",23.0,4.599999904632568,416.7,"true","5291");
		ArrayList<MySqlDataPrecision2> t2List=new ArrayList<MySqlDataPrecision2>();
		t2List.add(m4);
		t2List.add(m5);
		t2List.add(m6);
		return t2List;
	}
}
