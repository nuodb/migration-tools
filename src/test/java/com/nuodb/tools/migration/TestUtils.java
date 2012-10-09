package com.nuodb.tools.migration;


public class TestUtils {


    public final static String[] testArguments = new String[]{
            "dump",
            "--source.driver=com.mysql.jdbc.Driver",
            "--source.url=jdbc:mysql://localhost:3306/test",
            "--source.username=root",
            "--output.type=cvs",
            "--output.path=/tmp/"};



}
