# migration-tools #

[![Build Status](https://travis-ci.org/nuodb/migration-tools.png?branch=master)](https://travis-ci.org/nuodb/migration-tools)

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
        --help |
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