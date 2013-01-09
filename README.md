# migration-tools #

[![Build Status](https://secure.travis-ci.org/nuodb/migration-tools.png)](http://travis-ci.org/nuodb/migration-tools)

*A command-line interface for helping domain administrators manage backup and migration of their databases.*

This tool is designed to assist you in migrating data from supported SQL databases to a NuoDB database. Use nuodb-migration dump and nuodb-migration load to copy, normalize, and load data from an existing database (NuoDB or 3rd party) to a NuoDB database.  With the command-line interface, domain administrators will be able to perform the following database backup and migration tasks:

1. Migrate (generate) a schema to a target NuoDB database
2. Copy data from an existing database to a target NuoDB database
3. Dump data from an existing database
4. Load data to a target NuoDB database

*These functions are initially tested on MySQL, SQL Server, Oracle and PostgreSQL and should work with any JDBC-compliant database.*

## Building from Source ##

    $ git clone https://github.com/nuodb/migration-tools
    $ cd migration-tools/
    $ mvn install
    $ cd assembly/target/nuodb-migration/
    $ bin/nuodb-migration --help

## Synopsis ##

### Root command line options ###

    $ bin/nuodb-migration
        --help=<[dump] | [load] | [schema]> |
        --list |
        --config=<file>
        <[dump] | [load] | [schema]>

### Dump data from an existing database ###

    $ bin/nuodb-migration dump
        [source database connection, required]
            --source.driver<driver> |
            --source.url=<url> |
            --source.username=<username> |
            --source.password=<password> |
            --source.properties=<properties> |
            --source.catalog=<catalog> |
            --source.schema=<schema> |
        [output specification, required]
            --output.type=<output type> |
            --output.path=<output path> |
            --output.*=<attribute value> |
        [table names & query filters, optional]
            --table=<table> |
            --table.type=<table type> |
            --table.*.filter=<query filter> |
        [select statements, optional]
            --query=<query> |
        --time.zone=<time zone>

### Load data to a target NuoDB database ###

    $ bin/nuodb-migration load
        [target database connection, required]
            --target.url=<url> |
            --target.username=<username> |
            --target.password=<password> |
            --target.properties=<properties> |
            --target.schema=<schema> |
        [input specification, required]
            --input.type=<input type> |
            --input.path=<input path> |
            --input.*=<attribute value> |
        --time.zone=<time zone>

### Generate a schema for a target NuoDB database ###

    $ bin/nuodb-migration schema
        [source database connection, required]
            --source.driver=<driver> |
            --source.url=<url> |
            --source.username=<username> |
            --source.password=<password> |
            --source.properties=<properties> |
            --source.catalog=<catalog> |
            --source.schema=<schema> |
        [target database connection, optional]
            --target.url=<url> |
            --target.username=<username> |
            --target.password=<password> |
            --target.properties=<properties> |
            --target.schema=<schema> |
            --output.path=<output path> |
        [custom type declarations, optional]
            --type.name=<type name> |
            --type.code=<type code> |
            --type.size=<type size> |
            --type.precision=<type precision> |
            --type.scale=<type scale>
        [schema output options, optional]
            --meta.data.*=<[true] | [false]> |
            --script.type=<[drop] , [create]> |
            --group.scripts.by=<[table] | [meta.data]> |
            --identifier.normalizer=<[noop] | [standard] | [lower.case] | [upper.case]>

## Connect to Third-party Databases ##

To interface with third-party databases through JDBC-compliant drivers you should download & install appropriate JAR files.

You can add required dependency to pom.xml, then clean & package project:

    $ mvn clean install

The required JDBC driver JAR file will be download automatically to the assembly/target/nuodb-migration/jar/ directory

Alternatively, you can download & copy required JAR file to assembly/target/nuodb-migration/jar/ manually. For example, to install PostgreSQL JDBC4 Driver:

    $ mvn clean install
    $ curl http://jdbc.postgresql.org/download/postgresql-9.2-1001.jdbc4.jar > \
        assembly/target/nuodb-migration/jar/postgresql-9.2-1001.jdbc4.jar

To include MySQL JDBC Driver into assembly using Maven add the following dependency to pom.xml, then clean & package project:

    <?xml version="1.0" encoding="UTF-8"?>
    <project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        ...
        <dependencies>
            <dependency>
                <groupId>mysql</groupId>
                <artifactId>mysql-connector-java</artifactId>
                <version>5.1.20</version>
            </dependency>
        </dependencies>
        ...
    </project>

For Microsoft SQL Server you might use jTDS Type 4 JDBC Driver:

    <dependency>
        <groupId>net.sourceforge.jtds</groupId>
        <artifactId>jtds</artifactId>
        <version>1.2.4</version>
    <dependency>

For PostgreSQL add PostgreSQL Native Driver of the required version to the pom.xml file:

    <dependency>
        <groupId>postgresql</groupId>
        <artifactId>postgresql</artifactId>
        <version>9.0-801.jdbc4</version>
    </dependency>

## Examples ##

The following examples show how to dump MySQL to a file (in the first case), and an existing NuoDB database (in the second case).  The third case shows how to use the load command.

Example 1: Dump all tables from MySQL "enron" catalog in CSV format

    $ bin/nuodb-migration dump --source.driver=com.mysql.jdbc.Driver \
        --source.url=jdbc:mysql://localhost:3306/test --source.catalog=test \
        --source.username=<username> \
        --output.type=csv --output.path=/tmp/dump.cat

Example 2: Dump records from "hockey" table where "id" is not equal to 25 from NuoDB "test" catalog in BSON format

    $ bin/nuodb-migration dump --source.driver=com.nuodb.jdbc.Driver \
        --source.url=jdbc:com.nuodb://localhost/test \
        --source.username=<username> \
        --source.schema=hockey --table=hockey --table.hockey.filter=id<>25 \
        --output.path=/tmp/dump.cat --output.type=bson

Example 3: Load CSV data to the corresponding tables in NuoDB from /tmp/dump.cat

    $ bin/nuodb-migration load --target.url=jdbc:com.nuodb://localhost/test \
        --target.username=<username> --target.password=<password> \
        --input.path=/tmp/dump.cat

Example 4: Generate NuoDB schema from Oracle "test" database and output it to stdout stream. To save schema in a particular file on the file system add --output.path=<file> option, i.e. --output.path=/tmp/schema.sql

    $ bin/nuodb-migration schema --source.driver=oracle.jdbc.driver.OracleDriver \
        --source.url=jdbc:oracle:thin:@//localhost:1521/test \
        --source.username=<username> --source.password=<password> --source.schema=test

Example 5: Migrate schema from Microsoft SQL Server "test" database to a NuoDB database excluding generation of foreign keys and check constraints on table and column levels. Generated table names and column identifiers will be transformed using "standard" identifier normalizer (changed to upper case if unquoted).

    $ bin/nuodb-migration schema --source.driver=net.sourceforge.jtds.jdbc.Driver \
        --source.url=jdbc:jtds:sqlserver://localhost:1433/test \
        --source.username=<username> --source.password=<password> --source.schema=dbo \
        --target.url=jdbc:com.nuodb://localhost/test \
        --target.username=<username> --target.password=<password> --target.schema=hockey \
        --meta.data.foreign.key=false --meta.data.check.constraint=false \
        --identifier.normalizer=standard

## Command Line Options ##

### $ bin/nuodb-migration ###

<table>

<tr><td>
[--help=[command]]
</td><td>
Prints help contents on the requested command
</td></tr>

<tr><td>
[--list]
</td><td>
Lists available migration commands
</td></tr>

<tr><td>
&lt; [dump] | [load] | [schema] &gt;
</td><td>
Executes specified migration command (dump, load or schema)
</td></tr>

</table>


### $ bin/nuodb-migration dump ###

<table>
<tr><td colspan="2">
<b>Source database connection, required</b>
</td></tr>

<tr><td width="30%">
&nbsp;&nbsp;&nbsp;&nbsp;--source.driver=driver
</td><td>
JDBC driver class name
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;--source.url=url
</td><td>
Source database connection URL in the standard syntax jdbc:&lt;subprotocol&gt;:&lt;subname&gt;
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.username=[username]]
</td><td>
Source database username
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.password=[password]]
</td><td>
Source database password
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.properties=[properties]]
</td><td>
Additional connection properties encoded as URL query string "property1=value1&property2=value2"
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.catalog=[catalog]]
</td><td>
Default database catalog name to use
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.schema=[schema]]
</td><td>
Default database schema name to use
</td></tr>

<tr><td colspan="2">
<b>Output specification, required</b>
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;--output.type=output type
</td><td>
Output format type name (cvs, xml or bson)
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;--output.path=[output path]
</td><td>
Path on the file system to the .cat file
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--output.*=[attribute value]]
</td><td>
CSV output format attributes
<a href="https://github.com/nuodb/migration-tools/blob/master/core/src/main/java/com/nuodb/migration/resultset/format/csv/CsvAttributes.java">com.nuodb.migration.result.format.csv.CsvAttributes</a>
<ul>
<li>--output.csv.delimiter=,</li>
<li>--output.csv.quoting=false</li>
<li>--output.csv.quote="</li>
<li>--output.csv.escape=|</li>
<li>--output.csv.line.separator=\r\n</li>
</ul>
XML output format attributes
<a href="https://github.com/nuodb/migration-tools/blob/master/core/src/main/java/com/nuodb/migration/resultset/format/xml/XmlAttributes.java">com.nuodb.migration.result.format.xml.XmlAttributes</a>
<ul>
<li>--output.xml.encoding=UTF-8</li>
</ul>
</td></tr>

<tr><td colspan="2">
<b>Table names, types & query filters, optional</b>
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--table=table]
</td><td>
Table name
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--table.type=[table type]]
</td><td>
Comma separated types of tables (TABLE, VIEW, SYSTEM TABLE, GLOBAL TEMPORARY, ALIAS, SYNONYM, etc) to process, by default only TABLE is included into dump
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--table.*.filter=[query filter]]
</td><td>
Filters table records when appended to the query statement after the where clause
</td></tr>

<tr><td colspan="2">
<b>Select statements, optional</b>
</td></tr>
<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--query=query]
</td><td>
Select statement
</td></tr>

<tr><td>
[--time.zone=time zone]
</td><td>
Time zone option enables date columns to be dumped and reloaded between servers in different time zones, i.e. --time.zone=UTC
</td></tr>

</table>


### $ bin/nuodb-migration load ###

<table>

<tr><td colspan="2">
<b>Target database connection, required</b>
</td></tr>

<tr><td width="30%">
&nbsp;&nbsp;&nbsp;&nbsp;--target.url=url
</td><td>
Target database connection URL in format jdbc:com.nuodb://{BROKER}:{PORT}/{DATABASE}
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--target.username=[username]]
</td><td>
Target database username
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--target.password=[password]]
</td><td>
Target database password
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--target.properties=[properties]]
</td><td>
Additional connection properties encoded as URL query string "property1=value1&property2=value2"
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--target.schema=[schema]]
</td><td>
Default database schema name to use
</td></tr>

<tr><td colspan="2">
<b>Input specification, required</b>
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;--input.path=[input path]
</td><td>
Path on the file system to the .cat file
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--input.*=[attribute value]]
</td><td>
Input format attributes, same options as described under the <b>Output specification</b> of
<a href="#-binnuodb-migration-dump">$ bin/nuodb-migration dump</a>
</td></tr>

<tr><td>
[--time.zone=time zone]
</td><td>
Time zone option enables date columns to be dumped and reloaded between servers in different time zones, i.e. --time.zone=UTC
</td></tr>

</table>


### $ bin/nuodb-migration schema ###

<table>
<tr><td colspan="2">
<b>Source database connection, required</b>
</td></tr>

<tr><td width="30%">
&nbsp;&nbsp;&nbsp;&nbsp;--source.driver=driver
</td><td>
JDBC driver class name
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;--source.url=url
</td><td>
Source database connection URL in the standard syntax jdbc:&lt;subprotocol&gt;:&lt;subname&gt;
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.username=[username]]
</td><td>
Source database username
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.password=[password]]
</td><td>
Source database password
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.properties=[properties]]
</td><td>
Additional connection properties encoded as URL query string "property1=value1&property2=value2"
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.catalog=[catalog]]
</td><td>
Default database catalog name to use
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--source.schema=[schema]]
</td><td>
Default database schema name to use
</td></tr>

<tr><td colspan="2">
<b>Target database connection, optional</b>
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;--target.url=url
</td><td>
Target database connection URL in format jdbc:com.nuodb://{BROKER}:{PORT}/{DATABASE}
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--target.username=[username]]
</td><td>
Target database username
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--target.password=[password]]
</td><td>
Target database password
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--target.properties=[properties]]
</td><td>
Additional connection properties encoded as URL query string "property1=value1&property2=value2"
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--target.schema=[schema]]
</td><td>
Default database schema name to use
</td></tr>

<tr><td colspan="2">
<b>Script output, optional</b>
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;--output.path=output path
</td><td>
Path on the file system to the generated schema file, i.e. /tmp/schema.sql
</td></tr>

<tr><td colspan="2">
<b>Custom type declarations, optional</b>
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--type.name=type name]
</td><td>
SQL type name template, i.e. decimal({p},{s}) or varchar({n}), where {p} is a placeholder for a precision, {s} is a scale and {n} is a maximum size
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--type.code=type code]
</td><td>
Integer code of declared SQL type
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--type.size=[type size]]
</td><td>
Maximum size of custom data type
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--type.precision=[type precision]]
</td><td>
The maximum total number of decimal digits that can be stored, both to the left and to the right of the decimal point. Typically, type precision is in the range of 1 through the maximum precision of 38
</td></tr>

<tr><td>
&nbsp;&nbsp;&nbsp;&nbsp;[--type.scale=[type scale]]
</td><td>
The number of fractional digits for numeric data types
</td></tr>

<tr><td>
[--meta.data.*=[true | false]]
</td><td>
Enables or disables specific meta data type (catalog, schema, table, column, primary.key, index, foreign.key, check.constraint, auto.increment) for the generation, by default all objects are generated
</td></tr>

<tr><td>
[--group.scripts.by=[table | meta.data]]
</td><td>
Group generated DDL scripts, table by default
</td></tr>

<tr><td>
[--identifier.normalizer=[noop | standard | lower.case | upper.case]]
</td><td>
Identifier normalizer to use (noop, standard, lower.case, upper.case), default is noop
</td></tr>

</table>